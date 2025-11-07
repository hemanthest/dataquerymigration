package com.querymanager.service;

import com.querymanager.dto.MappingEntry;
import com.querymanager.dto.QueryDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class ExcelService {

    public static class FileUploadResult {
        private final List<QueryDTO> queries;
        private final List<MappingEntry> mappings;
        
        public FileUploadResult(List<QueryDTO> queries, List<MappingEntry> mappings) {
            this.queries = queries;
            this.mappings = mappings;
        }
        
        public List<QueryDTO> getQueries() {
            return queries;
        }
        
        public List<MappingEntry> getMappings() {
            return mappings;
        }
    }

    public FileUploadResult readFiles(MultipartFile queryFile, MultipartFile mappingFile) throws IOException {
        List<QueryDTO> queries = readQueryFile(queryFile);
        List<MappingEntry> mappings = readMappingFile(mappingFile);
        return new FileUploadResult(queries, mappings);
    }

    private static final String REPORTS_DIR = "reports";

    public ExcelService() {
        // Create reports directory if it doesn't exist
        try {
            Path reportsPath = Paths.get(REPORTS_DIR);
            if (!Files.exists(reportsPath)) {
                Files.createDirectories(reportsPath);
                log.info("Created reports directory: {}", reportsPath.toAbsolutePath());
            }
        } catch (IOException e) {
            log.error("Failed to create reports directory", e);
        }
    }

    public List<QueryDTO> readQueryFile(MultipartFile file) throws IOException {
        List<QueryDTO> queries = new ArrayList<>();

        try (InputStream is = file.getInputStream();
             Workbook workbook = new XSSFWorkbook(is)) {

            Sheet sheet = workbook.getSheetAt(0);

            // Skip header row
            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                if (row == null) continue;

                String queryName = getCellValueAsString(row.getCell(0));
                if (queryName == null || queryName.trim().isEmpty()) {
                    continue; // Skip rows without a query name
                }

                QueryDTO query = new QueryDTO();
                query.setQueryName(queryName);
                query.setDescription(getCellValueAsString(row.getCell(1)));
                query.setOriginalQuery(getCellValueAsString(row.getCell(2)));
                query.setUpdatedQuery(getCellValueAsString(row.getCell(3)));  // Read updated query
                query.setImpacted(false);

                queries.add(query);
            }
        }

        log.info("Read {} queries from query file", queries.size());
        return queries;
    }

    public List<MappingEntry> readMappingFile(MultipartFile file) throws IOException {
        List<MappingEntry> mappings = new ArrayList<>();

        try (InputStream is = file.getInputStream();
             Workbook workbook = new XSSFWorkbook(is)) {

            Sheet sheet = workbook.getSheetAt(0);

            // Skip header row
            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                if (row == null) continue;

                String deprecatedObject = getCellValueAsString(row.getCell(0));
                String newObject = getCellValueAsString(row.getCell(1));

                if (deprecatedObject == null || deprecatedObject.trim().isEmpty()) {
                    continue;
                }

                MappingEntry entry = parseMappingEntry(deprecatedObject, newObject);
                mappings.add(entry);
            }
        }

        log.info("Read {} mappings from mapping file", mappings.size());
        return mappings;
    }

    private MappingEntry parseMappingEntry(String deprecatedObject, String newObject) {
        MappingEntry entry = new MappingEntry();
        entry.setDeprecatedObject(deprecatedObject);
        entry.setNewObject(newObject);

        // Parse deprecated object
        if (deprecatedObject.contains(".")) {
            String[] parts = deprecatedObject.split("\\.", 2);
            entry.setDeprecatedTable(parts[0]);
            entry.setDeprecatedField(parts[1]);
        } else {
            entry.setDeprecatedTable(deprecatedObject);
            entry.setDeprecatedField(null);
        }

        // Parse new object
        if (newObject != null && newObject.contains(".")) {
            String[] parts = newObject.split("\\.", 2);
            entry.setNewTable(parts[0]);
            entry.setNewField(parts[1]);
        } else {
            entry.setNewTable(newObject);
            entry.setNewField(null);
        }

        return entry;
    }

    public byte[] generateImpactedReport(List<QueryDTO> impactedQueries) throws IOException {
        try (Workbook workbook = new XSSFWorkbook();
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            // Create header style
            CellStyle headerStyle = workbook.createCellStyle();
            Font headerFont = workbook.createFont();
            headerFont.setBold(true);
            headerStyle.setFont(headerFont);
            headerStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
            headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            // Sheet 1: Query List
            Sheet queryListSheet = workbook.createSheet("Query List");
            String[] queryListHeaders = {"Query Name", "Query Description", "Original Query"};
            createSheetWithData(queryListSheet, headerStyle, queryListHeaders, impactedQueries, 
                (row, query, rowNum) -> {
                    row.createCell(0).setCellValue(query.getQueryName());
                    row.createCell(1).setCellValue(query.getDescription());
                    row.createCell(2).setCellValue(query.getOriginalQuery());
                });

            // Sheet 2: Migration Results
            Sheet migrationSheet = workbook.createSheet("Migration Results");
            String[] migrationHeaders = {"Query Name", "Query Description", "Original Query", "Updated Query"};
            createSheetWithData(migrationSheet, headerStyle, migrationHeaders, impactedQueries,
                (row, query, rowNum) -> {
                    row.createCell(0).setCellValue(query.getQueryName());
                    row.createCell(1).setCellValue(query.getDescription());
                    row.createCell(2).setCellValue(query.getOriginalQuery());
                    row.createCell(3).setCellValue(query.getUpdatedQuery());
                });

            // Sheet 3: Zuora Update Results
            Sheet zuoraSheet = workbook.createSheet("Zuora Update Results");
            String[] zuoraHeaders = {"Query Name", "Query Description", "Original Query", "Updated Query", "Old URL", "New URL", "Status"};
            createSheetWithData(zuoraSheet, headerStyle, zuoraHeaders, impactedQueries,
                (row, query, rowNum) -> {
                    row.createCell(0).setCellValue(query.getQueryName());
                    row.createCell(1).setCellValue(query.getDescription());
                    row.createCell(2).setCellValue(query.getOriginalQuery());
                    row.createCell(3).setCellValue(query.getUpdatedQuery());
                    row.createCell(4).setCellValue(query.getOldUrl() != null ? query.getOldUrl() : "");
                    row.createCell(5).setCellValue(query.getNewUrl() != null ? query.getNewUrl() : "");
                    row.createCell(6).setCellValue(query.getStatus() != null ? query.getStatus() : "");
                });

            workbook.write(out);
            log.info("Generated report with {} queries across 3 sheets", impactedQueries.size());
            return out.toByteArray();
        }
    }

    @FunctionalInterface
    private interface RowWriter {
        void writeRow(Row row, QueryDTO query, int rowNum);
    }

    private void createSheetWithData(Sheet sheet, CellStyle headerStyle, String[] headers,
                                   List<QueryDTO> queries, RowWriter rowWriter) {
        // Create header row
        Row headerRow = sheet.createRow(0);
        for (int i = 0; i < headers.length; i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(headers[i]);
            cell.setCellStyle(headerStyle);
        }

        // Add data rows
        int rowNum = 1;
        for (QueryDTO query : queries) {
            Row row = sheet.createRow(rowNum);
            rowWriter.writeRow(row, query, rowNum);
            rowNum++;
        }

        // Auto-size columns
        for (int i = 0; i < headers.length; i++) {
            sheet.autoSizeColumn(i);
        }
    }

    public String saveImpactedReport(List<QueryDTO> impactedQueries) throws IOException {
        String fileName = "Impacted_DataQueries.xlsx";
        Path filePath = Paths.get(REPORTS_DIR, fileName);

        try (Workbook workbook = new XSSFWorkbook();
             FileOutputStream fileOut = new FileOutputStream(filePath.toFile())) {

            // Create header style
            CellStyle headerStyle = workbook.createCellStyle();
            Font headerFont = workbook.createFont();
            headerFont.setBold(true);
            headerStyle.setFont(headerFont);
            headerStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
            headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            // Sheet 1: Query List
            Sheet queryListSheet = workbook.createSheet("Query List");
            String[] queryListHeaders = {"Query Name", "Query Description", "Original Query"};
            createSheetWithData(queryListSheet, headerStyle, queryListHeaders, impactedQueries, 
                (row, query, rowNum) -> {
                    row.createCell(0).setCellValue(query.getQueryName());
                    row.createCell(1).setCellValue(query.getDescription());
                    row.createCell(2).setCellValue(query.getOriginalQuery());
                });

            // Sheet 2: Migration Results
            Sheet migrationSheet = workbook.createSheet("Migration Results");
            String[] migrationHeaders = {"Query Name", "Query Description", "Original Query", "Updated Query"};
            createSheetWithData(migrationSheet, headerStyle, migrationHeaders, impactedQueries,
                (row, query, rowNum) -> {
                    row.createCell(0).setCellValue(query.getQueryName());
                    row.createCell(1).setCellValue(query.getDescription());
                    row.createCell(2).setCellValue(query.getOriginalQuery());
                    row.createCell(3).setCellValue(query.getUpdatedQuery());
                });

            // Sheet 3: Zuora Update Results
            Sheet zuoraSheet = workbook.createSheet("Zuora Update Results");
            String[] zuoraHeaders = {"Query Name", "Query Description", "Original Query", "Updated Query", "Old URL", "New URL", "Status"};
            createSheetWithData(zuoraSheet, headerStyle, zuoraHeaders, impactedQueries,
                (row, query, rowNum) -> {
                    row.createCell(0).setCellValue(query.getQueryName());
                    row.createCell(1).setCellValue(query.getDescription());
                    row.createCell(2).setCellValue(query.getOriginalQuery());
                    row.createCell(3).setCellValue(query.getUpdatedQuery());
                    row.createCell(4).setCellValue(query.getOldUrl() != null ? query.getOldUrl() : "");
                    row.createCell(5).setCellValue(query.getNewUrl() != null ? query.getNewUrl() : "");
                    row.createCell(6).setCellValue(query.getStatus() != null ? query.getStatus() : "");
                });

            workbook.write(fileOut);
            log.info("Saved report with {} queries across 3 sheets to: {}", 
                impactedQueries.size(), filePath.toAbsolutePath());
            return filePath.toString();
        }
    }

    public String saveExtractedQueries(List<QueryDTO> queries) throws IOException {
        String fileName = "extracted_queries.xlsx";
        Path filePath = Paths.get(REPORTS_DIR, fileName);

        try (Workbook workbook = new XSSFWorkbook();
             FileOutputStream fileOut = new FileOutputStream(filePath.toFile())) {

            CellStyle headerStyle = workbook.createCellStyle();
            Font headerFont = workbook.createFont();
            headerFont.setBold(true);
            headerStyle.setFont(headerFont);
            headerStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
            headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            Sheet sheet = workbook.createSheet("Extracted Queries");
            String[] headers = {"queryName", "description", "query"};
            createSheetWithData(sheet, headerStyle, headers, queries, (row, query, rowNum) -> {
                row.createCell(0).setCellValue(query.getQueryName());
                row.createCell(1).setCellValue(query.getDescription());
                // Use originalQuery as the 'query' field if present
                String sql = query.getOriginalQuery() != null ? query.getOriginalQuery() : "";
                row.createCell(2).setCellValue(sql);
            });

            workbook.write(fileOut);
            log.info("Saved extracted queries report to: {}", filePath.toAbsolutePath());
            return filePath.toString();
        }
    }

    public String saveImpactedSimple(List<QueryDTO> impactedQueries) throws IOException {
        String fileName = "impacted_queries.xlsx";
        Path filePath = Paths.get(REPORTS_DIR, fileName);

        try (Workbook workbook = new XSSFWorkbook();
             FileOutputStream fileOut = new FileOutputStream(filePath.toFile())) {

            CellStyle headerStyle = workbook.createCellStyle();
            Font headerFont = workbook.createFont();
            headerFont.setBold(true);
            headerStyle.setFont(headerFont);
            headerStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
            headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            Sheet sheet = workbook.createSheet("Impacted Queries");
            String[] headers = {"queryName", "description", "originalQuery", "updatedQuery"};
            createSheetWithData(sheet, headerStyle, headers, impactedQueries, (row, query, rowNum) -> {
                row.createCell(0).setCellValue(query.getQueryName());
                row.createCell(1).setCellValue(query.getDescription());
                row.createCell(2).setCellValue(query.getOriginalQuery());
                row.createCell(3).setCellValue(query.getUpdatedQuery());
            });

            workbook.write(fileOut);
            log.info("Saved impacted queries report to: {}", filePath.toAbsolutePath());
            return filePath.toString();
        }
    }

    public String saveUpdateReport(List<QueryDTO> queries) throws IOException {
        String fileName = "Update_queries.xlsx";
        Path filePath = Paths.get(REPORTS_DIR, fileName);

        try (Workbook workbook = new XSSFWorkbook();
             FileOutputStream fileOut = new FileOutputStream(filePath.toFile())) {

            CellStyle headerStyle = workbook.createCellStyle();
            Font headerFont = workbook.createFont();
            headerFont.setBold(true);
            headerStyle.setFont(headerFont);
            headerStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
            headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            Sheet sheet = workbook.createSheet("Zuora Update Results");
            String[] headers = {"queryName", "description", "originalQuery", "updatedQuery", "oldUrl", "newUrl", "status"};
            createSheetWithData(sheet, headerStyle, headers, queries, (row, query, rowNum) -> {
                row.createCell(0).setCellValue(query.getQueryName());
                row.createCell(1).setCellValue(query.getDescription());
                row.createCell(2).setCellValue(query.getOriginalQuery());
                row.createCell(3).setCellValue(query.getUpdatedQuery());
                row.createCell(4).setCellValue(query.getOldUrl() != null ? query.getOldUrl() : "");
                row.createCell(5).setCellValue(query.getNewUrl() != null ? query.getNewUrl() : "");
                row.createCell(6).setCellValue(query.getStatus() != null ? query.getStatus() : "");
            });

            workbook.write(fileOut);
            log.info("Saved update queries report to: {}", filePath.toAbsolutePath());
            return filePath.toString();
        }
    }

    private String getCellValueAsString(Cell cell) {
        if (cell == null) {
            return "";
        }

        try {
            switch (cell.getCellType()) {
                case STRING:
                    return cell.getStringCellValue() != null ? cell.getStringCellValue().trim() : "";
                case NUMERIC:
                    if (DateUtil.isCellDateFormatted(cell)) {
                        return cell.getDateCellValue().toString();
                    }
                    return String.valueOf(cell.getNumericCellValue());
                case BOOLEAN:
                    return String.valueOf(cell.getBooleanCellValue());
                case FORMULA:
                    try {
                        return cell.getStringCellValue().trim();
                    } catch (Exception e) {
                        try {
                            return String.valueOf(cell.getNumericCellValue());
                        } catch (Exception ex) {
                            return cell.getCellFormula();
                        }
                    }
                case BLANK:
                    return "";
                default:
                    return "";
            }
        } catch (Exception e) {
            log.warn("Error reading cell value", e);
            return "";
        }
    }
}