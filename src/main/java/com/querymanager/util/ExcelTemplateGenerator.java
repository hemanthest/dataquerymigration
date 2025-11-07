package com.querymanager.util;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.FileOutputStream;
import java.io.IOException;

public class ExcelTemplateGenerator {
    public static void main(String[] args) {
        String filePath = "reports/zuora/queries_to_update.xlsx";
        
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Queries");
            
            // Create header row
            Row headerRow = sheet.createRow(0);
            String[] headers = {"Query Name", "Query Description", "Query", "Old URL", "New URL", "New Query Name", "Status"};
            
            // Style for headers
            CellStyle headerStyle = workbook.createCellStyle();
            headerStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
            headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            Font boldFont = workbook.createFont();
            boldFont.setBold(true);
            headerStyle.setFont(boldFont);
            
            // Add headers
            for (int i = 0; i < headers.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers[i]);
                cell.setCellStyle(headerStyle);
                sheet.autoSizeColumn(i);
            }
            
            // Add sample data
            Row dataRow = sheet.createRow(1);
            dataRow.createCell(0).setCellValue("Example Query");
            dataRow.createCell(1).setCellValue("Sample query for testing");
            dataRow.createCell(2).setCellValue("SELECT Id, Name, Status FROM Account LIMIT 10");
            
            // Auto-size columns
            for (int i = 0; i < headers.length; i++) {
                sheet.autoSizeColumn(i);
            }
            
            // Write to file
            try (FileOutputStream fos = new FileOutputStream(filePath)) {
                workbook.write(fos);
                System.out.println("Template created successfully at: " + filePath);
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}