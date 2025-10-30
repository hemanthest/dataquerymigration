package com.querymanager.service;

import com.querymanager.dto.MappingEntry;
import com.querymanager.dto.QueryDTO;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
@Slf4j
public class SqlMigrationService {

    /**
     * Result of query migration containing the migrated query and whether changes were made
     */
    private static class MigrationResult {
        private final String query;
        private final boolean hasChanges;

        public MigrationResult(String query, boolean hasChanges) {
            this.query = query;
            this.hasChanges = hasChanges;
        }

        public String getQuery() {
            return query;
        }

        public boolean hasChanges() {
            return hasChanges;
        }
    }

    public List<QueryDTO> migrateQueries(List<QueryDTO> queries, List<MappingEntry> mappings) {
        // Build efficient lookup structures
        Map<String, MappingEntry> fieldMappings = buildFieldMappingMap(mappings);
        Map<String, List<MappingEntry>> tableMappings = buildTableMappingMap(mappings);

        List<QueryDTO> impactedQueries = new ArrayList<>();

        for (QueryDTO query : queries) {
            try {
                MigrationResult result = migrateQuery(query.getOriginalQuery(), fieldMappings, tableMappings);

                if (result.hasChanges()) {
                    query.setUpdatedQuery(result.getQuery());
                    query.setImpacted(true);
                    impactedQueries.add(query);
                    log.debug("Query '{}' impacted and updated", query.getQueryName());
                }
            } catch (Exception e) {
                log.warn("Parse failed for query '{}': {} - Attempting fallback replacement", 
                    query.getQueryName(), e.getMessage());
                
                // Enhanced fallback: apply simple replacement with formatting preservation
                String originalQuery = query.getOriginalQuery();
                String fallbackUpdate = performSimpleReplacement(originalQuery, mappings);
                
                if (!fallbackUpdate.equals(originalQuery)) {
                    query.setUpdatedQuery(fallbackUpdate);
                    query.setImpacted(true);
                    impactedQueries.add(query);
                    log.info("Query '{}' updated via fallback replacement", query.getQueryName());
                    log.debug("Fallback - Original: {}", originalQuery.substring(0, Math.min(100, originalQuery.length())));
                    log.debug("Fallback - Updated: {}", fallbackUpdate.substring(0, Math.min(100, fallbackUpdate.length())));
                } else {
                    log.debug("Query '{}' unchanged after fallback (no deprecated objects found)", query.getQueryName());
                }
            }
        }

        log.info("Migration complete. {} out of {} queries impacted", impactedQueries.size(), queries.size());
        return impactedQueries;
    }

    private Map<String, MappingEntry> buildFieldMappingMap(List<MappingEntry> mappings) {
        return mappings.stream()
                .filter(MappingEntry::isFieldLevelMapping)
                .collect(Collectors.toMap(
                        m -> m.getDeprecatedTable().toLowerCase() + "." + m.getDeprecatedField().toLowerCase(),
                        m -> m,
                        (existing, replacement) -> replacement
                ));
    }

    private Map<String, List<MappingEntry>> buildTableMappingMap(List<MappingEntry> mappings) {
        Map<String, List<MappingEntry>> tableMap = new HashMap<>();

        for (MappingEntry mapping : mappings) {
            String table = mapping.getDeprecatedTable().toLowerCase();
            tableMap.computeIfAbsent(table, k -> new ArrayList<>()).add(mapping);
        }

        return tableMap;
    }

    private MigrationResult migrateQuery(String query, Map<String, MappingEntry> fieldMappings,
                                Map<String, List<MappingEntry>> tableMappings) throws JSQLParserException {

        // Sanitize the query before parsing
        String sanitizedQuery = sanitizeSQL(query);
        
        Statement statement = CCJSqlParserUtil.parse(sanitizedQuery);

        if (statement instanceof Select) {
            Select select = (Select) statement;
            QueryMigrationProcessor processor = new QueryMigrationProcessor(fieldMappings, tableMappings);
            processor.processSelect(select);
            
            if (processor.hasChanges()) {
                // Apply replacements to the ORIGINAL query (not sanitized) to preserve formatting
                String formattedResult = applyReplacementsWithFormatting(
                    query, 
                    processor.getReplacements()
                );
                return new MigrationResult(formattedResult, true);
            }
            
            return new MigrationResult(query, false);
        }

        return new MigrationResult(query, false);
    }
    
    /**
     * Sanitize SQL to improve parsing success rate.
     * This removes common issues like trailing commas, extra whitespace, and unprintable characters.
     */
    private String sanitizeSQL(String sql) {
        if (sql == null) {
            return null;
        }
        
        String result = sql;
        
        // Remove unprintable characters and normalize line endings
        result = result.replaceAll("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F\\x7F]", "");
        result = result.replaceAll("\\r\\n", "\n").replaceAll("\\r", "\n");
        
        // Remove trailing commas in SELECT lists (common Excel copy-paste issue)
        // Pattern: comma followed by whitespace/newlines then FROM/WHERE/GROUP/ORDER/LIMIT
        result = result.replaceAll(",\\s*\\n?\\s*(FROM|WHERE|GROUP\\s+BY|ORDER\\s+BY|LIMIT|HAVING|UNION)", "\n$1");
        
        // Remove trailing commas before closing parentheses
        result = result.replaceAll(",\\s*\\)", ")");
        
        // Normalize multiple spaces to single space (but preserve intentional indentation)
        result = result.replaceAll("(?m)^\\s+", "    "); // Normalize leading spaces to 4 spaces
        result = result.replaceAll(" {2,}", " "); // Multiple spaces to single space
        
        // Remove trailing whitespace from lines
        result = result.replaceAll("(?m)\\s+$", "");
        
        return result.trim();
    }
    
    /**
     * Apply replacements to the original query while preserving formatting and aliases
     */
    private String applyReplacementsWithFormatting(String originalQuery, Map<String, String> replacements) {
        String result = originalQuery;
        
        // Build table-only replacements (without qualified columns)
        Map<String, String> tableReplacements = new LinkedHashMap<>();
        Map<String, String> columnReplacements = new LinkedHashMap<>();
        Map<String, String> aliasReplacements = new LinkedHashMap<>();
        
        for (Map.Entry<String, String> entry : replacements.entrySet()) {
            String oldValue = entry.getKey();
            String newValue = entry.getValue();
            
            if (oldValue.contains(".") && newValue.contains(".")) {
                // This is a qualified column replacement (e.g., amendment.id -> order.id)
                columnReplacements.put(oldValue, newValue);
                
                // Extract table and column parts
                String[] oldParts = oldValue.split("\\.", 2);
                String[] newParts = newValue.split("\\.", 2);
                
                if (oldParts.length == 2 && newParts.length == 2) {
                    String oldTable = oldParts[0];
                    String oldColumn = oldParts[1];
                    String newTable = newParts[0];
                    String newColumn = newParts[1];
                    
                    // Generate alias-based column name patterns (e.g., AmendmentId -> OrdersId)
                    String oldAliasPattern = capitalize(oldTable) + capitalize(oldColumn);
                    String newAliasPattern = capitalize(newTable) + capitalize(newColumn);
                    columnReplacements.put(oldAliasPattern, newAliasPattern);
                }
            } else if (!oldValue.contains(".")) {
                // Simple table replacement
                tableReplacements.put(oldValue, newValue);
                
                // Generate alias replacement: first 3 letters of new table name (lowercase)
                String newAlias = generateAlias(newValue);
                aliasReplacements.put(oldValue.toLowerCase(), newAlias);
            }
        }
        
        // Step 1: Extract actual aliases from FROM/JOIN clauses
        Map<String, String> actualAliasToTable = new HashMap<>();
        Map<String, String> oldAliasToNewAlias = new HashMap<>();
        Map<String, String> newAliasToNewTable = new HashMap<>();
        
        for (Map.Entry<String, String> entry : tableReplacements.entrySet()) {
            String oldTable = entry.getKey();
            String newTable = entry.getValue();
            String newAlias = aliasReplacements.get(oldTable.toLowerCase());
            
            // Find the actual alias used in the query for this table
            Pattern findAliasPattern = Pattern.compile(
                "(?i)(FROM|JOIN)\\s+" + Pattern.quote(oldTable) + "\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\\b",
                Pattern.CASE_INSENSITIVE
            );
            
            java.util.regex.Matcher aliasMatcher = findAliasPattern.matcher(result);
            if (aliasMatcher.find()) {
                String actualOldAlias = aliasMatcher.group(2);
                actualAliasToTable.put(actualOldAlias.toLowerCase(), oldTable);
                oldAliasToNewAlias.put(actualOldAlias.toLowerCase(), newAlias);
                newAliasToNewTable.put(newAlias.toLowerCase(), newTable);
            }
        }
        
        // Step 2: Replace table names AND their aliases in FROM and JOIN clauses
        for (Map.Entry<String, String> entry : tableReplacements.entrySet()) {
            String oldTable = entry.getKey();
            String newTable = entry.getValue();
            String newAlias = aliasReplacements.get(oldTable.toLowerCase());
            
            // Match: FROM/JOIN TableName [optional AS] alias
            // Replace with: FROM/JOIN NewTable newAlias
            String fromJoinPatternWithAlias = "(?i)(FROM|JOIN)\\s+" + Pattern.quote(oldTable) + 
                "\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\\b";
            
            result = result.replaceAll(fromJoinPatternWithAlias, "$1 " + newTable + " " + newAlias);
            
            // Also handle case without explicit alias (table used as its own alias)
            String fromJoinPatternNoAlias = "(?i)(FROM|JOIN)\\s+" + Pattern.quote(oldTable) + "\\b(?!\\s+[a-zA-Z])";
            result = result.replaceAll(fromJoinPatternNoAlias, "$1 " + newTable);
        }
        
        // Step 3: Replace qualified column names with mapping (e.g., ame.Name -> ord.OrderNumber)
        // This handles both with and without alias prefix
        List<Map.Entry<String, String>> sortedColumnReplacements = columnReplacements.entrySet().stream()
            .filter(e -> e.getKey().contains(".") && e.getValue().contains("."))
            .sorted((a, b) -> Integer.compare(b.getKey().length(), a.getKey().length()))
            .collect(Collectors.toList());
        
        for (Map.Entry<String, String> entry : sortedColumnReplacements) {
            String oldQualified = entry.getKey(); // e.g., "amendment.name"
            String newQualified = entry.getValue(); // e.g., "orders.ordernumber"
            
            String[] oldParts = oldQualified.split("\\.", 2);
            String[] newParts = newQualified.split("\\.", 2);
            
            if (oldParts.length == 2 && newParts.length == 2) {
                String oldTable = oldParts[0];
                String oldColumn = oldParts[1];
                String newTable = newParts[0];
                String newColumn = newParts[1];
                
                // Get the new alias for the new table (if it has one)
                String newAlias = aliasReplacements.get(oldTable.toLowerCase());
                if (newAlias == null) {
                    newAlias = generateAlias(newTable);
                }
                
                // Find the actual alias for this old table
                for (Map.Entry<String, String> aliasEntry : oldAliasToNewAlias.entrySet()) {
                    String oldAlias = aliasEntry.getKey();
                    String tableForAlias = actualAliasToTable.get(oldAlias);
                    
                    if (tableForAlias != null && tableForAlias.equalsIgnoreCase(oldTable)) {
                        // Replace: oldAlias.oldColumn -> newAlias.newColumn
                        String oldAliasColumnPattern = "(?i)\\b" + Pattern.quote(oldAlias) + "\\." + Pattern.quote(oldColumn) + "\\b";
                        result = result.replaceAll(oldAliasColumnPattern, newAlias + "." + newColumn);
                    }
                }
                
                // Also replace direct table.column references with alias.column
                String oldTableColumnPattern = "(?i)\\b" + Pattern.quote(oldTable) + "\\." + Pattern.quote(oldColumn) + "\\b";
                result = result.replaceAll(oldTableColumnPattern, newAlias + "." + newColumn);
                
                // Replace new table name references with alias (Orders.Id -> ord.Id)
                String newTableColumnPattern = "(?i)\\b" + Pattern.quote(newTable) + "\\." + Pattern.quote(newColumn) + "\\b";
                result = result.replaceAll(newTableColumnPattern, newAlias + "." + newColumn);
            }
        }
        
        // Step 4: Dynamically replace AS aliases based on new table + new column
        // Pattern: alias.column AS SomeAlias -> newAlias.newColumn AS NewTableNewColumn
        Pattern asPattern = Pattern.compile(
            "(?i)\\b([a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\b",
            Pattern.CASE_INSENSITIVE
        );
        
        java.util.regex.Matcher asMatcher = asPattern.matcher(result);
        StringBuffer sb = new StringBuffer();
        
        while (asMatcher.find()) {
            String foundAlias = asMatcher.group(1);
            String foundColumn = asMatcher.group(2);
            // group(3) is the current AS name, which we'll replace dynamically
            
            // Check if this is a new alias that was generated from a mapped table
            String newTable = newAliasToNewTable.get(foundAlias.toLowerCase());
            if (newTable != null) {
                // Generate new AS alias: NewTableNewColumn (capitalized)
                String newAsName = capitalize(newTable) + capitalize(foundColumn);
                asMatcher.appendReplacement(sb, foundAlias + "." + foundColumn + " AS " + newAsName);
            } else {
                // Keep original (unmapped table)
                asMatcher.appendReplacement(sb, asMatcher.group(0));
            }
        }
        asMatcher.appendTail(sb);
        result = sb.toString();
        
        return result;
    }
    
    /**
     * Generate alias from table name (first 3 letters, lowercase)
     */
    private String generateAlias(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            return tableName;
        }
        
        String cleaned = tableName.trim();
        int length = Math.min(3, cleaned.length());
        return cleaned.substring(0, length).toLowerCase();
    }
    
    /**
     * Capitalize first letter of a string
     */
    private String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
    }

    private String performSimpleReplacement(String query, List<MappingEntry> mappings) {
        String result = query;

        // Build table and column replacement maps
        Map<String, String> tableReplacements = new HashMap<>();
        Map<String, String> columnReplacements = new HashMap<>();
        Map<String, String> aliasReplacements = new HashMap<>();
        
        for (MappingEntry mapping : mappings) {
            String deprecated = mapping.getDeprecatedObject();
            String newObj = mapping.getNewObject();
            
            if (deprecated.contains(".") && newObj.contains(".")) {
                // Column-level mapping
                columnReplacements.put(deprecated, newObj);
                
                // Generate alias patterns
                String[] oldParts = deprecated.split("\\.", 2);
                String[] newParts = newObj.split("\\.", 2);
                
                if (oldParts.length == 2 && newParts.length == 2) {
                    String oldAlias = capitalize(oldParts[0]) + capitalize(oldParts[1]);
                    String newAlias = capitalize(newParts[0]) + capitalize(newParts[1]);
                    columnReplacements.put(oldAlias, newAlias);
                }
            } else if (!deprecated.contains(".")) {
                // Table-level mapping
                tableReplacements.put(deprecated, newObj);
                
                // Generate alias: first 3 letters of new table name
                String newAlias = generateAlias(newObj);
                aliasReplacements.put(deprecated.toLowerCase(), newAlias);
            }
        }
        
        // Step 1: Extract actual aliases from FROM/JOIN clauses BEFORE replacement
        Map<String, String> actualAliasToTable = new HashMap<>();
        Map<String, String> oldAliasToNewAlias = new HashMap<>();
        Map<String, String> newAliasToNewTable = new HashMap<>();
        
        for (Map.Entry<String, String> entry : tableReplacements.entrySet()) {
            String oldTable = entry.getKey();
            String newTable = entry.getValue();
            String newAlias = aliasReplacements.get(oldTable.toLowerCase());
            
            // Find the actual alias used in the query for this table
            Pattern findAliasPattern = Pattern.compile(
                "(?i)(FROM|JOIN)\\s+" + Pattern.quote(oldTable) + "\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\\b",
                Pattern.CASE_INSENSITIVE
            );
            
            java.util.regex.Matcher aliasMatcher = findAliasPattern.matcher(result);
            if (aliasMatcher.find()) {
                String actualOldAlias = aliasMatcher.group(2);
                actualAliasToTable.put(actualOldAlias.toLowerCase(), oldTable);
                oldAliasToNewAlias.put(actualOldAlias.toLowerCase(), newAlias);
                newAliasToNewTable.put(newAlias.toLowerCase(), newTable);
            }
        }
        
        // Step 2: Replace table names AND their aliases in FROM/JOIN clauses
        for (Map.Entry<String, String> entry : tableReplacements.entrySet()) {
            String oldTable = entry.getKey();
            String newTable = entry.getValue();
            String newAlias = aliasReplacements.get(oldTable.toLowerCase());
            
            // Match: FROM/JOIN TableName [optional AS] alias - replace both
            String fromJoinPatternWithAlias = "(?i)(FROM|JOIN)\\s+" + Pattern.quote(oldTable) + 
                "\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\\b";
            result = result.replaceAll(fromJoinPatternWithAlias, "$1 " + newTable + " " + newAlias);
            
            // Handle case without explicit alias
            String fromJoinPatternNoAlias = "(?i)(FROM|JOIN)\\s+" + Pattern.quote(oldTable) + "\\b(?!\\s+[a-zA-Z])";
            result = result.replaceAll(fromJoinPatternNoAlias, "$1 " + newTable);
        }
        
        // Step 3: Replace qualified column names with mapping (e.g., ame.Name -> ord.OrderNumber)
        List<Map.Entry<String, String>> sortedColumnReplacements = columnReplacements.entrySet().stream()
            .filter(e -> e.getKey().contains(".") && e.getValue().contains("."))
            .sorted((a, b) -> Integer.compare(b.getKey().length(), a.getKey().length()))
            .collect(Collectors.toList());
        
        for (Map.Entry<String, String> entry : sortedColumnReplacements) {
            String oldQualified = entry.getKey();
            String newQualified = entry.getValue();
            
            String[] oldParts = oldQualified.split("\\.", 2);
            String[] newParts = newQualified.split("\\.", 2);
            
            if (oldParts.length == 2 && newParts.length == 2) {
                String oldTable = oldParts[0];
                String oldColumn = oldParts[1];
                String newTable = newParts[0];
                String newColumn = newParts[1];
                
                // Get the new alias for the new table (if it has one)
                String newAlias = aliasReplacements.get(oldTable.toLowerCase());
                if (newAlias == null) {
                    newAlias = generateAlias(newTable);
                }
                
                // Find the actual alias for this old table
                for (Map.Entry<String, String> aliasEntry : oldAliasToNewAlias.entrySet()) {
                    String oldAlias = aliasEntry.getKey();
                    String tableForAlias = actualAliasToTable.get(oldAlias);
                    
                    if (tableForAlias != null && tableForAlias.equalsIgnoreCase(oldTable)) {
                        // Replace: oldAlias.oldColumn -> newAlias.newColumn
                        String oldAliasColumnPattern = "(?i)\\b" + Pattern.quote(oldAlias) + "\\." + Pattern.quote(oldColumn) + "\\b";
                        result = result.replaceAll(oldAliasColumnPattern, newAlias + "." + newColumn);
                    }
                }
                
                // Also replace direct table.column references with alias.column
                String oldTableColumnPattern = "(?i)\\b" + Pattern.quote(oldTable) + "\\." + Pattern.quote(oldColumn) + "\\b";
                result = result.replaceAll(oldTableColumnPattern, newAlias + "." + newColumn);
                
                // Replace new table name references with alias (Orders.Id -> ord.Id)
                String newTableColumnPattern = "(?i)\\b" + Pattern.quote(newTable) + "\\." + Pattern.quote(newColumn) + "\\b";
                result = result.replaceAll(newTableColumnPattern, newAlias + "." + newColumn);
            }
        }
        
        // Step 4: Dynamically replace AS aliases based on new table + new column
        Pattern asPattern = Pattern.compile(
            "(?i)\\b([a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\b",
            Pattern.CASE_INSENSITIVE
        );
        
        java.util.regex.Matcher asMatcher = asPattern.matcher(result);
        StringBuffer sb = new StringBuffer();
        
        while (asMatcher.find()) {
            String foundAlias = asMatcher.group(1);
            String foundColumn = asMatcher.group(2);
            // group(3) is the current AS name, which we'll replace dynamically
            
            // Check if this is a new alias that was generated from a mapped table
            String newTable = newAliasToNewTable.get(foundAlias.toLowerCase());
            if (newTable != null) {
                // Generate new AS alias: NewTableNewColumn (capitalized)
                String newAsName = capitalize(newTable) + capitalize(foundColumn);
                asMatcher.appendReplacement(sb, foundAlias + "." + foundColumn + " AS " + newAsName);
            } else {
                // Keep original (unmapped table)
                asMatcher.appendReplacement(sb, asMatcher.group(0));
            }
        }
        asMatcher.appendTail(sb);
        result = sb.toString();
        
        return result;
    }

    /**
     * Processor for migrating SQL queries
     */
    private static class QueryMigrationProcessor {
        private final Map<String, MappingEntry> fieldMappings;
        private final Map<String, List<MappingEntry>> tableMappings;
        private final Map<String, String> aliasToTableMap = new HashMap<>();
        private final Map<String, String> aliasToOriginalTableMap = new HashMap<>();
        private final Map<String, String> tableToNewTableMap = new HashMap<>();
        private boolean hasChanges = false;  // Track if any migrations were performed
        private final Map<String, String> replacements = new LinkedHashMap<>();  // Track all replacements made

        public QueryMigrationProcessor(Map<String, MappingEntry> fieldMappings,
                                      Map<String, List<MappingEntry>> tableMappings) {
            this.fieldMappings = fieldMappings;
            this.tableMappings = tableMappings;
        }

        public boolean hasChanges() {
            return hasChanges;
        }
        
        public Map<String, String> getReplacements() {
            return replacements;
        }

        public void processSelect(Select select) {
            if (select instanceof PlainSelect) {
                PlainSelect plainSelect = (PlainSelect) select;
                processPlainSelect(plainSelect);
            } else if (select instanceof SetOperationList) {
                SetOperationList setOp = (SetOperationList) select;
                for (Select s : setOp.getSelects()) {
                    processSelect(s);
                }
            }
        }

        private void processPlainSelect(PlainSelect plainSelect) {
            // First pass: collect all tables and aliases
            collectTables(plainSelect);
            
            // Analyze field usage to determine table mappings
            analyzeFieldUsage(plainSelect);
            
            // Update tables in FROM and JOIN clauses
            updateTables(plainSelect);
            
            // Update columns
            updateColumns(plainSelect);
        }

        private void collectTables(PlainSelect plainSelect) {
            if (plainSelect.getFromItem() != null) {
                collectFromItem(plainSelect.getFromItem());
            }

            if (plainSelect.getJoins() != null) {
                for (Join join : plainSelect.getJoins()) {
                    collectFromItem(join.getRightItem());
                }
            }
        }

        private void collectFromItem(FromItem fromItem) {
            if (fromItem instanceof Table) {
                Table table = (Table) fromItem;
                String tableName = table.getName();
                Alias alias = table.getAlias();
                
                if (alias != null && alias.getName() != null) {
                    aliasToTableMap.put(alias.getName().toLowerCase(), tableName.toLowerCase());
                    aliasToOriginalTableMap.put(alias.getName().toLowerCase(), tableName.toLowerCase());
                } else {
                    aliasToTableMap.put(tableName.toLowerCase(), tableName.toLowerCase());
                    aliasToOriginalTableMap.put(tableName.toLowerCase(), tableName.toLowerCase());
                }
            }
        }

        private void analyzeFieldUsage(PlainSelect plainSelect) {
            // Analyze SELECT items
            if (plainSelect.getSelectItems() != null) {
                for (SelectItem<?> item : plainSelect.getSelectItems()) {
                    Expression expr = item.getExpression();
                    if (expr != null) {
                        analyzeExpression(expr);
                    }
                }
            }

            // Analyze WHERE clause
            if (plainSelect.getWhere() != null) {
                analyzeExpression(plainSelect.getWhere());
            }

            // Analyze GROUP BY
            if (plainSelect.getGroupBy() != null) {
                GroupByElement groupBy = plainSelect.getGroupBy();
                ExpressionList<?> groupByExpressions = groupBy.getGroupByExpressionList();
                if (groupByExpressions != null && groupByExpressions.size() > 0) {
                    for (Expression expr : groupByExpressions) {
                        analyzeExpression(expr);
                    }
                }
            }

            // Analyze ORDER BY
            if (plainSelect.getOrderByElements() != null) {
                for (OrderByElement orderBy : plainSelect.getOrderByElements()) {
                    analyzeExpression(orderBy.getExpression());
                }
            }
        }

        private void analyzeExpression(Expression expr) {
            if (expr instanceof Column) {
                Column column = (Column) expr;
                analyzeColumn(column);
            } else if (expr instanceof BinaryExpression) {
                BinaryExpression binExpr = (BinaryExpression) expr;
                analyzeExpression(binExpr.getLeftExpression());
                analyzeExpression(binExpr.getRightExpression());
            } else if (expr instanceof ParenthesedExpressionList) {
                ParenthesedExpressionList<?> paren = (ParenthesedExpressionList<?>) expr;
                for (Expression e : paren) {
                    analyzeExpression(e);
                }
            } else if (expr instanceof Function) {
                Function func = (Function) expr;
                if (func.getParameters() != null && func.getParameters().size() > 0) {
                    for (Expression e : func.getParameters()) {
                        analyzeExpression(e);
                    }
                }
            } else if (expr instanceof Between) {
                Between between = (Between) expr;
                analyzeExpression(between.getLeftExpression());
                analyzeExpression(between.getBetweenExpressionStart());
                analyzeExpression(between.getBetweenExpressionEnd());
            } else if (expr instanceof InExpression) {
                InExpression inExpr = (InExpression) expr;
                analyzeExpression(inExpr.getLeftExpression());
            }
        }

        private void analyzeColumn(Column column) {
            Table table = column.getTable();
            if (table == null) return;

            String tableOrAlias = table.getName().toLowerCase();
            String columnName = column.getColumnName().toLowerCase();

            String actualTable = aliasToTableMap.getOrDefault(tableOrAlias, tableOrAlias);
            String lookupKey = actualTable + "." + columnName;

            MappingEntry mapping = fieldMappings.get(lookupKey);
            if (mapping != null) {
                tableToNewTableMap.put(actualTable, mapping.getNewTable());
            } else {
                List<MappingEntry> possibleMappings = tableMappings.get(actualTable);
                if (possibleMappings != null && !possibleMappings.isEmpty()) {
                    tableToNewTableMap.putIfAbsent(actualTable, possibleMappings.get(0).getNewTable());
                }
            }
        }

        private void updateTables(PlainSelect plainSelect) {
            if (plainSelect.getFromItem() instanceof Table) {
                Table table = (Table) plainSelect.getFromItem();
                updateTable(table);
            }

            if (plainSelect.getJoins() != null) {
                for (Join join : plainSelect.getJoins()) {
                    if (join.getRightItem() instanceof Table) {
                        Table table = (Table) join.getRightItem();
                        updateTable(table);
                    }
                    
                    // Update JOIN conditions
                    if (join.getOnExpressions() != null) {
                        for (Expression expr : join.getOnExpressions()) {
                            updateExpressionColumns(expr);
                        }
                    }
                }
            }
        }

        private void updateTable(Table table) {
            String tableName = table.getName().toLowerCase();
            String newTableName = tableToNewTableMap.get(tableName);
            
            if (newTableName != null) {
                // Track replacement for formatting preservation
                replacements.put(tableName, newTableName);
                
                table.setName(newTableName);
                hasChanges = true;  // Mark that we made a change
                
                Alias alias = table.getAlias();
                // Update aliasToTableMap to point to the new table name for future lookups
                // but keep aliasToOriginalTableMap unchanged for field mapping lookups
                if (alias != null && alias.getName() != null) {
                    aliasToTableMap.put(alias.getName().toLowerCase(), newTableName.toLowerCase());
                } else {
                    aliasToTableMap.put(newTableName.toLowerCase(), newTableName.toLowerCase());
                    // Also update for non-aliased table
                    aliasToOriginalTableMap.put(newTableName.toLowerCase(), tableName);
                }
            }
        }

        private void updateColumns(PlainSelect plainSelect) {
            // Update SELECT items
            if (plainSelect.getSelectItems() != null) {
                for (SelectItem<?> item : plainSelect.getSelectItems()) {
                    Expression expr = item.getExpression();
                    if (expr != null) {
                        updateExpressionColumns(expr);
                    }
                }
            }

            // Update WHERE clause
            if (plainSelect.getWhere() != null) {
                updateExpressionColumns(plainSelect.getWhere());
            }

            // Update GROUP BY
            if (plainSelect.getGroupBy() != null) {
                GroupByElement groupBy = plainSelect.getGroupBy();
                ExpressionList<?> groupByExpressions = groupBy.getGroupByExpressionList();
                if (groupByExpressions != null && groupByExpressions.size() > 0) {
                    for (Expression expr : groupByExpressions) {
                        updateExpressionColumns(expr);
                    }
                }
            }

            // Update ORDER BY
            if (plainSelect.getOrderByElements() != null) {
                for (OrderByElement orderBy : plainSelect.getOrderByElements()) {
                    updateExpressionColumns(orderBy.getExpression());
                }
            }
        }

        private void updateExpressionColumns(Expression expr) {
            if (expr instanceof Column) {
                updateColumn((Column) expr);
            } else if (expr instanceof BinaryExpression) {
                BinaryExpression binExpr = (BinaryExpression) expr;
                updateExpressionColumns(binExpr.getLeftExpression());
                updateExpressionColumns(binExpr.getRightExpression());
            } else if (expr instanceof ParenthesedExpressionList) {
                ParenthesedExpressionList<?> paren = (ParenthesedExpressionList<?>) expr;
                for (Expression e : paren) {
                    updateExpressionColumns(e);
                }
            } else if (expr instanceof Function) {
                Function func = (Function) expr;
                if (func.getParameters() != null && func.getParameters().size() > 0) {
                    for (Expression e : func.getParameters()) {
                        updateExpressionColumns(e);
                    }
                }
            } else if (expr instanceof Between) {
                Between between = (Between) expr;
                updateExpressionColumns(between.getLeftExpression());
                updateExpressionColumns(between.getBetweenExpressionStart());
                updateExpressionColumns(between.getBetweenExpressionEnd());
            } else if (expr instanceof InExpression) {
                InExpression inExpr = (InExpression) expr;
                updateExpressionColumns(inExpr.getLeftExpression());
            }
        }

        private void updateColumn(Column column) {
            Table table = column.getTable();
            if (table == null) return;

            String tableOrAlias = table.getName().toLowerCase();
            String columnName = column.getColumnName().toLowerCase();

            // Try to find the original table name
            // First check if this is an alias we know about
            String originalTable = aliasToOriginalTableMap.get(tableOrAlias);
            
            // If not found, it might be a direct table reference (no alias)
            // Check if this table name itself is a deprecated table
            if (originalTable == null) {
                originalTable = tableOrAlias;
            }
            
            // Check if the table itself has been renamed and update the column's table reference
            String newTableName = tableToNewTableMap.get(originalTable);
            if (newTableName != null) {
                // Track table.column replacement when only table changes
                String oldRef = tableOrAlias + "." + columnName;
                String newRef = newTableName + "." + columnName;
                replacements.put(oldRef, newRef);
                
                // Update the table reference in the column (e.g., Amendment.Status -> Order.Status)
                table.setName(newTableName);
                hasChanges = true;
            }
            
            // Then check for field-level mappings
            String lookupKey = originalTable + "." + columnName;
            MappingEntry mapping = fieldMappings.get(lookupKey);
            if (mapping != null) {
                // Track full column replacement (table.oldColumn -> newTable.newColumn)
                String oldRef = tableOrAlias + "." + columnName;
                String newRef = (mapping.getNewTable() != null ? mapping.getNewTable() : tableOrAlias) + "." + mapping.getNewField();
                replacements.put(oldRef, newRef);
                
                // Update the column name to the new field name
                column.setColumnName(mapping.getNewField());
                // Also update the table name if it's different from what we already set
                if (mapping.getNewTable() != null && !mapping.getNewTable().equalsIgnoreCase(table.getName())) {
                    table.setName(mapping.getNewTable());
                }
                hasChanges = true;  // Mark that we made a change
            }
        }
    }
}