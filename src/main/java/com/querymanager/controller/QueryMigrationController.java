package com.querymanager.controller;

import com.querymanager.dto.MappingEntry;
import com.querymanager.dto.MigrationResponse;
import com.querymanager.dto.QueryDTO;
// removed unused import
import com.querymanager.service.ExcelService;
import com.querymanager.service.SqlMigrationService;
// removed unused import

import java.io.File;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
public class QueryMigrationController {

    private final ExcelService excelService;
    private final SqlMigrationService sqlMigrationService;
    // Removed unused dependencies to satisfy linter



    @PostMapping(value = "/migrate-queries", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<MigrationResponse> migrateQueries(
            @RequestParam("queryFile") MultipartFile queryFile,
            @RequestParam("mappingFile") MultipartFile mappingFile) {

        try {
            log.info("Received migration request with queryFile: {}, mappingFile: {}",
                    queryFile.getOriginalFilename(), mappingFile.getOriginalFilename());

            // Validate files
            validateFile(queryFile, "queryFile");
            validateFile(mappingFile, "mappingFile");

            // Read input files
            List<QueryDTO> queries = excelService.readQueryFile(queryFile);
            List<MappingEntry> mappings = excelService.readMappingFile(mappingFile);

            log.info("Loaded {} queries and {} mappings", queries.size(), mappings.size());

            // Process queries
            List<QueryDTO> impactedQueries = sqlMigrationService.migrateQueries(queries, mappings);

            if (impactedQueries.isEmpty()) {
                log.info("No queries were impacted by the migration");
                MigrationResponse response = MigrationResponse.builder()
                        .success(true)
                        .message("Migration completed successfully. No queries were impacted.")
                        .totalQueries(queries.size())
                        .impactedQueries(0)
                        .build();
                return ResponseEntity.ok(response);
            }

            // Save simple impacted report to reports folder
            String reportPath = excelService.saveImpactedSimple(impactedQueries);

            log.info("Successfully generated report with {} impacted queries", impactedQueries.size());

            // Prepare response
            MigrationResponse response = MigrationResponse.builder()
                    .success(true)
                    .message("Migration completed successfully.")
                    .totalQueries(queries.size())
                    .impactedQueries(impactedQueries.size())
                    .reportFilePath(reportPath)
                    .build();

            try {
                return ResponseEntity.ok(response);
            } catch (Exception e) {
                // If we can't send the response but work was done successfully,
                // log it as info since it's likely just a client disconnect
                if (e.getCause() instanceof java.io.IOException) {
                    log.info("Client disconnected after successful migration. Response not sent but work was completed. Error: {}", e.getMessage());
                    // The container will handle the client disconnect
                    throw e;
                }
                // For other exceptions, treat as error
                log.error("Error sending response for successful migration", e);
                throw e;
            }

        } catch (IllegalArgumentException e) {
            log.error("Validation error: {}", e.getMessage());
            MigrationResponse response = MigrationResponse.builder()
                    .success(false)
                    .message("Validation failed")
                    .error(e.getMessage())
                    .build();
            return ResponseEntity.badRequest().body(response);
        } catch (Exception e) {
            log.error("Error processing migration request", e);
            MigrationResponse response = MigrationResponse.builder()
                    .success(false)
                    .message("Migration failed due to an internal error")
                    .error(e.getMessage())
                    .build();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    private void validateFile(MultipartFile file, String paramName) {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException(paramName + " is required and cannot be empty");
        }

        String filename = file.getOriginalFilename();
        if (filename == null || !filename.toLowerCase().endsWith(".xlsx")) {
            throw new IllegalArgumentException(paramName + " must be an .xlsx file");
        }
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("SQL Query Migrator Service is running");
    }

    @GetMapping("/download-report")
    public ResponseEntity<Resource> downloadReport(@RequestParam("filePath") String filePath) {
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                log.error("Report file not found: {}", filePath);
                return ResponseEntity.notFound().build();
            }

            Resource resource = new FileSystemResource(file);
            
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + file.getName());
            
            return ResponseEntity.ok()
                    .headers(headers)
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(resource);
                    
        } catch (Exception e) {
            log.error("Error downloading report", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}