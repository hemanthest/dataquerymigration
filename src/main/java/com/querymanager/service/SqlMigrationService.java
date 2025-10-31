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
        
        log.debug("=== applyReplacementsWithFormatting START ===");
        log.debug("Replacements map: {}", replacements);
        log.debug("Original query (first 200 chars): {}", originalQuery.substring(0, Math.min(200, originalQuery.length())));
        
        // Build table-only replacements (without qualified columns)
        Map<String, String> tableReplacements = new LinkedHashMap<>();
        Map<String, String> columnReplacements = new LinkedHashMap<>();
        Map<String, String> aliasReplacements = new LinkedHashMap<>();
        
        // Multi-target table support: Map source table to multiple target tables
        Map<String, List<String>> sourceToTargetTables = new LinkedHashMap<>();
        Map<String, String> targetTableAliases = new LinkedHashMap<>();
        Map<String, Map<String, String>> columnToTargetTable = new LinkedHashMap<>();
        
        for (Map.Entry<String, String> entry : replacements.entrySet()) {
            String oldValue = entry.getKey().trim();  // Trim whitespace
            String newValue = entry.getValue().trim();  // Trim whitespace
            
            if (oldValue.contains(".") && newValue.contains(".")) {
                // This is a qualified column replacement (e.g., amendment.id -> orders.id)
                columnReplacements.put(oldValue, newValue);
                
                // Extract table and column parts
                String[] oldParts = oldValue.split("\\.", 2);
                String[] newParts = newValue.split("\\.", 2);
                
                if (oldParts.length == 2 && newParts.length == 2) {
                    String oldTable = oldParts[0].toLowerCase();  // Normalize to lowercase
                    String oldColumn = oldParts[1];
                    String newTable = newParts[0];
                    String newColumn = newParts[1];
                    
                    // Track source -> target table mappings (use lowercase key for consistency)
                    sourceToTargetTables.computeIfAbsent(oldTable, k -> new ArrayList<>());
                    if (!sourceToTargetTables.get(oldTable).contains(newTable)) {
                        sourceToTargetTables.get(oldTable).add(newTable);
                    }
                    
                    // Track which columns map to which target table
                    columnToTargetTable.computeIfAbsent(oldTable, k -> new LinkedHashMap<>());
                    columnToTargetTable.get(oldTable).put(oldColumn, newTable);
                    
                    // Generate alias-based column name patterns (e.g., AmendmentId -> OrdersId)
                    String oldAliasPattern = capitalize(oldTable) + capitalize(oldColumn);
                    String newAliasPattern = capitalize(newTable) + capitalize(newColumn);
                    columnReplacements.put(oldAliasPattern, newAliasPattern);
                }
            } else if (!oldValue.contains(".")) {
                // Simple table replacement
                tableReplacements.put(oldValue, newValue);
            }
        }
        
        // Infer table-level replacements from column-level mappings
        // This handles cases where only column mappings exist (e.g., amendment.id → orders.id)
        for (Map.Entry<String, List<String>> entry : sourceToTargetTables.entrySet()) {
            String sourceTable = entry.getKey();
            List<String> targetTables = entry.getValue();
            
            // Add to tableReplacements for the primary (first) target table
            if (!tableReplacements.containsKey(sourceTable) && !targetTables.isEmpty()) {
                tableReplacements.put(sourceTable, targetTables.get(0));
            }
        }
        
        // Generate unique aliases for multi-target tables
        for (Map.Entry<String, List<String>> entry : sourceToTargetTables.entrySet()) {
            String sourceTable = entry.getKey();
            List<String> targetTables = entry.getValue();
            
            if (targetTables.size() == 1) {
                // Single target table: use standard alias
                String targetTable = targetTables.get(0);
                String alias = generateAlias(targetTable);
                targetTableAliases.put(targetTable, alias);
                aliasReplacements.put(sourceTable.toLowerCase(), alias);
            } else {
                // Multiple target tables: sort by name length (shorter first) for consistent alias assignment
                List<String> sortedTargets = new ArrayList<>(targetTables);
                sortedTargets.sort(Comparator.comparingInt(String::length));
                
                // Generate unique aliases
                for (int i = 0; i < sortedTargets.size(); i++) {
                    String targetTable = sortedTargets.get(i);
                    String alias = generateUniqueAlias(targetTable, sortedTargets, i);
                    targetTableAliases.put(targetTable, alias);
                    
                    // First (shortest) table gets the base alias mapping
                    if (i == 0) {
                        aliasReplacements.put(sourceTable.toLowerCase(), alias);
                    }
                }
                
                // Update sourceToTargetTables to use sorted list
                sourceToTargetTables.put(sourceTable, sortedTargets);
            }
        }
        
        // Step 1: Extract actual aliases FROM/JOIN clauses
        Map<String, String> actualAliasToTable = new HashMap<>();
        Map<String, String> oldAliasToNewAlias = new HashMap<>();
        Map<String, String> newAliasToNewTable = new HashMap<>();
        
        // Debug: Log sourceToTargetTables
        log.debug("sourceToTargetTables: {}", sourceToTargetTables);
        log.debug("targetTableAliases: {}", targetTableAliases);
        
        // For multi-target tables, track the primary alias
        for (Map.Entry<String, List<String>> entry : sourceToTargetTables.entrySet()) {
            String oldTable = entry.getKey();
            List<String> targetTables = entry.getValue();
            
            // Find the actual alias used in the query for this source table
            // Create case-insensitive pattern for table name matching
            String tablePattern = oldTable.chars()
                .mapToObj(c -> {
                    char ch = (char) c;
                    if (Character.isLetter(ch)) {
                        return "[" + Character.toLowerCase(ch) + Character.toUpperCase(ch) + "]";
                    } else {
                        return Pattern.quote(String.valueOf(ch));
                    }
                })
                .collect(java.util.stream.Collectors.joining());
            Pattern findAliasPattern = Pattern.compile(
                "(FROM|JOIN)\\s+" + tablePattern + "\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\\b",
                Pattern.DOTALL
            );
            
            java.util.regex.Matcher aliasMatcher = findAliasPattern.matcher(result);
            if (aliasMatcher.find()) {
                String actualOldAlias = aliasMatcher.group(2);
                log.debug("Found alias '{}' for table '{}'", actualOldAlias, oldTable);
                actualAliasToTable.put(actualOldAlias.toLowerCase(), oldTable);
                
                // Map old alias to primary target table's alias
                String primaryTarget = targetTables.get(0);
                String primaryAlias = targetTableAliases.get(primaryTarget);
                oldAliasToNewAlias.put(actualOldAlias.toLowerCase(), primaryAlias);
                log.debug("Mapping old alias '{}' -> new alias '{}' for target '{}'", actualOldAlias, primaryAlias, primaryTarget);
                
                // Map all target table aliases to their table names
                for (String targetTable : targetTables) {
                    String targetAlias = targetTableAliases.get(targetTable);
                    newAliasToNewTable.put(targetAlias.toLowerCase(), targetTable);
                }
            } else {
                log.warn("No alias found for table '{}' in query", oldTable);
            }
        }
        
        // Step 2: Replace table names AND their aliases in FROM/JOIN clauses + add additional JOINs
        log.debug("=== STEP 2 START ===");
        log.debug("Query before Step 2: {}", result.substring(0, Math.min(300, result.length())));
        
        for (Map.Entry<String, List<String>> entry : sourceToTargetTables.entrySet()) {
            String oldTable = entry.getKey();
            List<String> targetTables = entry.getValue();
            String primaryTarget = targetTables.get(0);
            String primaryAlias = targetTableAliases.get(primaryTarget);
            
            log.debug("Processing table '{}' -> primaryTarget='{}', primaryAlias='{}'", oldTable, primaryTarget, primaryAlias);
            
            // Replace the original FROM/JOIN with the primary target table
            // Create case-insensitive pattern for table name
            String tablePatternReplace = oldTable.chars()
                .mapToObj(c -> {
                    char ch = (char) c;
                    if (Character.isLetter(ch)) {
                        return "[" + Character.toLowerCase(ch) + Character.toUpperCase(ch) + "]";
                    } else {
                        return Pattern.quote(String.valueOf(ch));
                    }
                })
                .collect(java.util.stream.Collectors.joining());
            String fromJoinPatternWithAlias = "(FROM|JOIN)\\s+" + tablePatternReplace + 
                "\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*)";
            
            log.debug("Step 2: Replacing '{}' -> '{} {}'", oldTable, primaryTarget, primaryAlias);
            log.debug("Pattern: {}", fromJoinPatternWithAlias);
            String beforeReplace = result;
            result = result.replaceAll(fromJoinPatternWithAlias, "$1 " + primaryTarget + " " + primaryAlias);
            if (beforeReplace.equals(result)) {
                log.warn("Step 2: Pattern did NOT match anything for table '{}'", oldTable);
            } else {
                log.debug("Step 2: Successfully replaced JOIN clause for '{}'", oldTable);
            }
            
            // Also handle case without explicit alias
            String fromJoinPatternNoAlias = "(FROM|JOIN)\\s+" + tablePatternReplace + "\\b(?!\\s+[a-zA-Z])";
            result = result.replaceAll(fromJoinPatternNoAlias, "$1 " + primaryTarget);
            
            log.debug("After basic replacement, query snippet: {}", result.substring(0, Math.min(300, result.length())));
            
            // Add JOIN clauses for additional target tables (insert before WHERE clause)
            if (targetTables.size() > 1) {
                log.debug("Adding additional JOINs for {} target tables", targetTables.size());
                StringBuilder additionalJoins = new StringBuilder();
                
                // Find the ID column from primary table mappings
                String primaryIdColumn = "Id"; // default
                for (Map.Entry<String, String> colEntry : columnReplacements.entrySet()) {
                    String key = colEntry.getKey();
                    String value = colEntry.getValue();
                    if (key.contains(".") && value.contains(".")) {
                        String[] keyParts = key.split("\\.", 2);
                        String[] valueParts = value.split("\\.", 2);
                        if (keyParts[0].equalsIgnoreCase(oldTable) && 
                            valueParts[0].equalsIgnoreCase(primaryTarget) &&
                            keyParts[1].equalsIgnoreCase("id")) {
                            primaryIdColumn = valueParts[1]; // Get the actual ID column name
                            break;
                        }
                    }
                }
                
                for (int i = 1; i < targetTables.size(); i++) {
                    String additionalTarget = targetTables.get(i);
                    String additionalAlias = targetTableAliases.get(additionalTarget);
                    
                    // Foreign key column name: {SingularPrimaryTableName}Id (e.g., OrderId for Orders table)
                    // Use singular form to match alias convention: ord.Id -> orda.OrderId (not OrdersId)
                    String singularPrimaryTable = singularize(primaryTarget);
                    String foreignKeyColumn = singularPrimaryTable + capitalize(primaryIdColumn);
                    
                    log.debug("Generating additional JOIN for table '{}' with alias '{}', FK column: {}", 
                        additionalTarget, additionalAlias, foreignKeyColumn);
                    // Generate JOIN: JOIN OrderAction orda ON ord.Id = orda.OrderId
                    additionalJoins.append("\nJOIN ").append(additionalTarget).append(" ").append(additionalAlias);
                    additionalJoins.append(" ON ").append(primaryAlias).append(".").append(primaryIdColumn);
                    additionalJoins.append(" = ").append(additionalAlias).append(".").append(foreignKeyColumn);
                }
                
                log.debug("Additional JOINs generated: {}", additionalJoins.toString());
                
                // Insert before WHERE clause
                Pattern wherePattern = Pattern.compile("(?i)\\bWHERE\\b", Pattern.CASE_INSENSITIVE);
                java.util.regex.Matcher whereMatcher = wherePattern.matcher(result);
                if (whereMatcher.find()) {
                    log.debug("WHERE clause found at position {}, inserting JOINs before it", whereMatcher.start());
                    result = result.substring(0, whereMatcher.start()) + additionalJoins.toString() + "\n" + result.substring(whereMatcher.start());
                    log.debug("After JOIN insertion: {}", result.substring(0, Math.min(400, result.length())));
                } else {
                    log.debug("No WHERE clause found, appending JOINs at end");
                    // No WHERE clause, append at end
                    result += additionalJoins.toString();
                }
            }
        }
        
        // Step 2.5: Replace old alias references with new alias for NON-ROUTED columns only
        // This handles cases like "b.AccountId" in ON clauses that aren't in columnReplacements
        // BUT we need to be careful not to replace columns that will be routed to different targets in Step 3
        log.debug("oldAliasToNewAlias map: {}", oldAliasToNewAlias);
        
        // Build set of columns that will be routed in Step 3 (these should NOT be replaced in Step 2.5)
        Set<String> routedColumns = new HashSet<>();
        for (Map.Entry<String, String> entry : columnReplacements.entrySet()) {
            String key = entry.getKey();
            if (key.contains(".")) {
                String[] parts = key.split("\\.", 2);
                if (parts.length == 2) {
                    // Store as "oldtable.column" in lowercase for matching
                    routedColumns.add(parts[0].toLowerCase() + "." + parts[1].toLowerCase());
                }
            }
        }
        log.debug("Columns that will be routed in Step 3: {}", routedColumns);
        
        // For multi-target scenarios, we skip Step 2.5 entirely and let Step 3 handle ALL column routing
        // This is because Step 2.5 would incorrectly route columns to the primary table
        boolean isMultiTarget = sourceToTargetTables.values().stream().anyMatch(list -> list.size() > 1);
        
        if (!isMultiTarget) {
            // Single target: safe to do global alias replacement
            for (Map.Entry<String, String> entry : oldAliasToNewAlias.entrySet()) {
                String oldAlias = entry.getKey();
                String newAlias = entry.getValue();
                
                log.debug("Step 2.5: Replacing all '{}.' with '{}.''", oldAlias, newAlias);
                // Replace oldAlias. with newAlias. for ALL columns
                String globalAliasPattern = "(?i)\\b" + Pattern.quote(oldAlias) + "\\.";
                result = result.replaceAll(globalAliasPattern, newAlias + ".");
            }
        } else {
            log.debug("Step 2.5: Skipping global alias replacement (multi-target scenario - Step 3 will handle routing)");
        }
        
        // Step 3: Replace qualified column names with routing to correct target table
        log.debug("=== STEP 3 START: Column Routing ===");
        log.debug("columnReplacements: {}", columnReplacements);
        
        List<Map.Entry<String, String>> sortedColumnReplacements = columnReplacements.entrySet().stream()
            .filter(e -> e.getKey().contains(".") && e.getValue().contains("."))
            .sorted((a, b) -> Integer.compare(b.getKey().length(), a.getKey().length()))
            .collect(Collectors.toList());
        
        log.debug("Sorted column replacements to process: {}", sortedColumnReplacements.size());
        
        for (Map.Entry<String, String> entry : sortedColumnReplacements) {
            String oldQualified = entry.getKey(); // e.g., "amendment.name"
            String newQualified = entry.getValue(); // e.g., "orders.ordernumber"
            
            log.debug("Processing column replacement: {} -> {}", oldQualified, newQualified);
            
            String[] oldParts = oldQualified.split("\\.", 2);
            String[] newParts = newQualified.split("\\.", 2);
            
            if (oldParts.length == 2 && newParts.length == 2) {
                String oldTable = oldParts[0];
                String oldColumn = oldParts[1];
                String newTable = newParts[0];
                String newColumn = newParts[1];
                
                // Get the correct alias for this specific target table
                String newAlias = targetTableAliases.get(newTable);
                if (newAlias == null) {
                    newAlias = generateAlias(newTable);
                }
                
                // Find the actual old alias used in the query
                for (Map.Entry<String, String> aliasEntry : actualAliasToTable.entrySet()) {
                    String oldAlias = aliasEntry.getKey();
                    String sourceTable = aliasEntry.getValue();
                    
                    if (sourceTable != null && sourceTable.equalsIgnoreCase(oldTable)) {
                        // Replace: oldAlias.oldColumn -> newAlias.newColumn
                        // This routes the column to the correct target table alias
                        // Create case-insensitive pattern for oldColumn
                        String columnPattern = oldColumn.chars()
                            .mapToObj(c -> {
                                char ch = (char) c;
                                if (Character.isLetter(ch)) {
                                    return "[" + Character.toLowerCase(ch) + Character.toUpperCase(ch) + "]";
                                } else {
                                    return Pattern.quote(String.valueOf(ch));
                                }
                            })
                            .collect(java.util.stream.Collectors.joining());
                        
                        String oldAliasColumnPattern = "\\b" + Pattern.quote(oldAlias) + "\\." + columnPattern + "\\b";
                        String beforeReplace = result;
                        result = result.replaceAll(oldAliasColumnPattern, newAlias + "." + newColumn);
                        if (!beforeReplace.equals(result)) {
                            log.debug("Replaced {}.{} -> {}.{}", oldAlias, oldColumn, newAlias, newColumn);
                        }
                    }
                }
                
                // Also replace direct table.column references with alias.column
                String oldTableColumnPattern = "(?i)\\b" + Pattern.quote(oldTable) + "\\." + Pattern.quote(oldColumn) + "\\b";
                result = result.replaceAll(oldTableColumnPattern, newAlias + "." + newColumn);
                
                // Replace new table name references with alias (Orders.Id -> ord.Id)
                String newTableColumnPattern = "(?i)\\b" + Pattern.quote(newTable) + "\\." + Pattern.quote(newColumn) + "\\b";
                result = result.replaceAll(newTableColumnPattern, newAlias + "." + newColumn);
                
                // CRITICAL: After Step 2.5, newAlias.oldColumn might exist (e.g., ord.Name)
                // We need to replace it with newAlias.newColumn (e.g., ord.OrderNumber)
                // This handles single-target scenarios where Step 2.5 already changed the alias
                if (!oldColumn.equalsIgnoreCase(newColumn)) {
                    String oldColumnPattern = oldColumn.chars()
                        .mapToObj(c -> {
                            char ch = (char) c;
                            if (Character.isLetter(ch)) {
                                return "[" + Character.toLowerCase(ch) + Character.toUpperCase(ch) + "]";
                            } else {
                                return Pattern.quote(String.valueOf(ch));
                            }
                        })
                        .collect(java.util.stream.Collectors.joining());
                    
                    String newAliasOldColumnPattern = "\\b" + Pattern.quote(newAlias) + "\\." + oldColumnPattern + "\\b";
                    String beforeReplace = result;
                    result = result.replaceAll(newAliasOldColumnPattern, newAlias + "." + newColumn);
                    if (!beforeReplace.equals(result)) {
                        log.debug("Replaced {}.{} -> {}.{} (column name change after Step 2.5)", newAlias, oldColumn, newAlias, newColumn);
                    }
                }
            }
        }
        
        // Step 4: Dynamically replace AS aliases based on new table + new column
        // Pattern: alias.column AS SomeAlias -> newAlias.newColumn AS NewTableNewColumn
        // Use \\s+ to match any whitespace including newlines
        log.debug("=== STEP 4 START: AS Alias Replacement ===");
        log.debug("newAliasToNewTable map: {}", newAliasToNewTable);
        
        Pattern asPattern = Pattern.compile(
            "(?i)\\b([a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\b",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );
        
        java.util.regex.Matcher asMatcher = asPattern.matcher(result);
        StringBuffer sb = new StringBuffer();
        
        while (asMatcher.find()) {
            String foundAlias = asMatcher.group(1);
            String foundColumn = asMatcher.group(2);
            String currentAsName = asMatcher.group(3);
            
            log.debug("Found AS pattern: {}.{} AS {}", foundAlias, foundColumn, currentAsName);
            
            // Check if this is a new alias that was generated from a mapped table
            String newTable = newAliasToNewTable.get(foundAlias.toLowerCase());
            if (newTable != null) {
                // Check if the column name itself changed (e.g., Name -> OrderNumber)
                // If it did, the new column name is self-explanatory, so we don't need AS
                // Pattern: if foundColumn is different from the original column name, remove AS
                
                // Check if this column had a name change by looking at columnReplacements
                boolean columnNameChanged = false;
                String originalTable = actualAliasToTable.get(foundAlias.toLowerCase());
                if (originalTable != null) {
                    // Look for a mapping like "amendment.name -> Orders.OrderNumber" where the column name changes
                    for (Map.Entry<String, String> colEntry : columnReplacements.entrySet()) {
                        String key = colEntry.getKey();
                        String value = colEntry.getValue();
                        if (key.contains(".") && value.contains(".")) {
                            String[] keyParts = key.split("\\.", 2);
                            String[] valueParts = value.split("\\.", 2);
                            if (keyParts.length == 2 && valueParts.length == 2) {
                                // Check if this mapping is for the current table and column
                                // originalTable="amendment", newTable="Orders", foundColumn="OrderNumber"
                                // We're looking for: amendment.name -> Orders.OrderNumber
                                if (keyParts[0].equalsIgnoreCase(originalTable) &&
                                    valueParts[0].equalsIgnoreCase(newTable) &&
                                    valueParts[1].equalsIgnoreCase(foundColumn)) {
                                    // Found the mapping - check if column name changed
                                    // Compare: name vs OrderNumber
                                    if (!keyParts[1].equalsIgnoreCase(valueParts[1])) {
                                        columnNameChanged = true;
                                        log.debug("Detected column name change: {}.{} -> {}.{}", 
                                            keyParts[0], keyParts[1], valueParts[0], valueParts[1]);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                
                if (columnNameChanged) {
                    // Column name changed (e.g., Name -> OrderNumber), just use the new column name without AS
                    log.debug("Column name changed, removing AS clause: {}.{}", foundAlias, foundColumn);
                    asMatcher.appendReplacement(sb, foundAlias + "." + foundColumn);
                } else {
                    // Column name same, generate new AS alias: Singular(NewTable) + ColumnName (capitalized)
                    // Example: Orders + Id -> OrderId (not OrdersId)
                    String singularTable = singularize(newTable);
                    String newAsName = capitalize(singularTable) + capitalize(foundColumn);
                    log.debug("Replacing AS: {} -> {}", currentAsName, newAsName);
                    asMatcher.appendReplacement(sb, foundAlias + "." + foundColumn + " AS " + newAsName);
                }
            } else {
                log.debug("Keeping original AS (alias '{}' not in newAliasToNewTable)", foundAlias);
                // Keep original (unmapped table)
                asMatcher.appendReplacement(sb, asMatcher.group(0));
            }
        }
        asMatcher.appendTail(sb);
        result = sb.toString();
        
        log.debug("=== FINAL RESULT ===");
        log.debug("Final query: {}", result);
        
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
     * Generate unique alias for multi-target table scenarios
     * Example: Orders -> ord, OrdersAction -> orda (adds suffix to avoid conflict)
     */
    private String generateUniqueAlias(String tableName, List<String> allTargetTables, int index) {
        if (tableName == null || tableName.isEmpty()) {
            return tableName;
        }
        
        String baseAlias = generateAlias(tableName);
        
        // First table gets the base alias
        if (index == 0) {
            return baseAlias;
        }
        
        // Collect all aliases generated so far
        Set<String> usedAliases = new HashSet<>();
        for (int i = 0; i < index; i++) {
            String previousTable = allTargetTables.get(i);
            // Recursively generate to get the actual alias used
            String previousAlias = generateUniqueAlias(previousTable, allTargetTables, i);
            usedAliases.add(previousAlias);
        }
        
        // If base alias conflicts, add letter suffix
        if (usedAliases.contains(baseAlias)) {
            // Append letter suffix: 'a', 'b', 'c'...
            char suffix = 'a';
            String uniqueAlias = baseAlias + suffix;
            while (usedAliases.contains(uniqueAlias)) {
                suffix++;
                uniqueAlias = baseAlias + suffix;
            }
            return uniqueAlias;
        }
        
        return baseAlias;
    }
    
    /**
     * Capitalize first letter of a string (preserves rest of the string casing)
     */
    private String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
    
    /**
     * Convert plural table name to singular for AS alias generation
     * Example: Orders -> Order, OrdersAction -> OrderAction
     */
    private String singularize(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            return tableName;
        }
        
        // Remove trailing 's' if present
        if (tableName.endsWith("s") && tableName.length() > 1) {
            return tableName.substring(0, tableName.length() - 1);
        }
        
        return tableName;
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
                // Track table.column replacement using ORIGINAL TABLE NAME, not alias
                String oldRef = originalTable + "." + columnName;
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
                // Track full column replacement using ORIGINAL TABLE NAME, not alias
                String oldRef = originalTable + "." + columnName;
                String newRef = (mapping.getNewTable() != null ? mapping.getNewTable() : originalTable) + "." + mapping.getNewField();
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