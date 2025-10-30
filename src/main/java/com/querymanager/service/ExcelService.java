package com.querymanager.service;

import com.querymanager.dto.MappingEntry;
import com.querymanager.dto.QueryDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
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

                QueryDTO query = new QueryDTO();
                query.setQueryName(getCellValueAsString(row.getCell(0)));
                query.setDescription(getCellValueAsString(row.getCell(1)));
                query.setOriginalQuery(getCellValueAsString(row.getCell(2)));
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

            Sheet sheet = workbook.createSheet("Impacted Queries");

            // Create header style
            CellStyle headerStyle = workbook.createCellStyle();
            Font headerFont = workbook.createFont();
            headerFont.setBold(true);
            headerStyle.setFont(headerFont);
            headerStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
            headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            // Create header row
            Row headerRow = sheet.createRow(0);
            String[] headers = {"Query Name", "Query Description", "Actual Query", "Updated Query"};
            for (int i = 0; i < headers.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers[i]);
                cell.setCellStyle(headerStyle);
            }

            // Add data rows
            int rowNum = 1;
            for (QueryDTO query : impactedQueries) {
                Row row = sheet.createRow(rowNum++);
                row.createCell(0).setCellValue(query.getQueryName());
                row.createCell(1).setCellValue(query.getDescription());
                row.createCell(2).setCellValue(query.getOriginalQuery());
                row.createCell(3).setCellValue(query.getUpdatedQuery());
            }

            // Auto-size columns
            for (int i = 0; i < headers.length; i++) {
                sheet.autoSizeColumn(i);
            }

            workbook.write(out);
            log.info("Generated report with {} impacted queries", impactedQueries.size());
            return out.toByteArray();
        }
    }

    public String saveImpactedReport(List<QueryDTO> impactedQueries) throws IOException {
        String fileName = "Impacted_DataQueries.xlsx";
        Path filePath = Paths.get(REPORTS_DIR, fileName);

        try (Workbook workbook = new XSSFWorkbook();
             FileOutputStream fileOut = new FileOutputStream(filePath.toFile())) {

            Sheet sheet = workbook.createSheet("Impacted Queries");

            // Create header style
            CellStyle headerStyle = workbook.createCellStyle();
            Font headerFont = workbook.createFont();
            headerFont.setBold(true);
            headerStyle.setFont(headerFont);
            headerStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
            headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            // Create header row
            Row headerRow = sheet.createRow(0);
            String[] headers = {"Query Name", "Query Description", "Actual Query", "Updated Query"};
            for (int i = 0; i < headers.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers[i]);
                cell.setCellStyle(headerStyle);
            }

            // Add data rows
            int rowNum = 1;
            for (QueryDTO query : impactedQueries) {
                Row row = sheet.createRow(rowNum++);
                row.createCell(0).setCellValue(query.getQueryName());
                row.createCell(1).setCellValue(query.getDescription());
                row.createCell(2).setCellValue(query.getOriginalQuery());
                row.createCell(3).setCellValue(query.getUpdatedQuery());
            }

            // Auto-size columns
            for (int i = 0; i < headers.length; i++) {
                sheet.autoSizeColumn(i);
            }

            workbook.write(fileOut);
            log.info("Saved report to: {}", filePath.toAbsolutePath());
            return filePath.toString();
        }
    }

    private String getCellValueAsString(Cell cell) {
        if (cell == null) {
            return "";
        }

        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue();
            case NUMERIC:
                return String.valueOf(cell.getNumericCellValue());
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                return cell.getCellFormula();
            default:
                return "";
        }
    }
}