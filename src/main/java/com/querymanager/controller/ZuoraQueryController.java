package com.querymanager.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import com.querymanager.dto.MigrationResponse;
import com.querymanager.dto.QueryDTO;
import com.querymanager.service.ExcelService;
import com.querymanager.service.SqlMigrationService;
import com.querymanager.service.ZuoraService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/queries")
@RequiredArgsConstructor
@Slf4j
public class ZuoraQueryController {

    private final ZuoraService zuoraService;
    private final SqlMigrationService sqlMigrationService;
    private final ExcelService excelService;

    @GetMapping("/extract")
    public ResponseEntity<List<QueryDTO>> extractQueries() {
        log.info("Received request to extract queries from Zuora");
        List<QueryDTO> queries = zuoraService.extractQueries();
        log.info("Successfully extracted {} queries", queries.size());
        try {
            String path = excelService.saveExtractedQueries(queries);
            log.info("Extracted queries saved to: {}", path);
        } catch (Exception e) {
            log.warn("Failed to save extracted queries report", e);
        }
        return ResponseEntity.ok(queries);
    }

    @PostMapping(value = "/update", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<List<QueryDTO>> updateQueries(@RequestParam("file") MultipartFile file) {
        if (file == null || file.isEmpty()) {
            log.warn("Received empty or null file");
            return ResponseEntity.badRequest().build();
        }

        log.info("Received file: {} for query updates", file.getOriginalFilename());
        
        try {
            List<QueryDTO> impactedQueries = excelService.readQueryFile(file);
            if (impactedQueries.isEmpty()) {
                log.warn("No queries found in the uploaded file");
                return ResponseEntity.badRequest().build();
            }

            log.info("Processing {} queries for update", impactedQueries.size());
            List<QueryDTO> results = zuoraService.updateQueries(impactedQueries);
            log.info("Successfully processed {} queries", results.size());
            try {
                String path = excelService.saveUpdateReport(results);
                log.info("Update report saved to: {}", path);
            } catch (Exception e) {
                log.warn("Failed to save update report", e);
            }
            return ResponseEntity.ok(results);
        } catch (Exception e) {
            log.error("Error processing query update file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/migrate")
    public ResponseEntity<MigrationResponse> migrateQueries(
            @RequestParam("queryFile") MultipartFile queryFile,
            @RequestParam("mappingFile") MultipartFile mappingFile) {
        log.info("Received migration request with queryFile: {}, mappingFile: {}", 
                queryFile.getOriginalFilename(), mappingFile.getOriginalFilename());
        MigrationResponse response = sqlMigrationService.migrateQueries(queryFile, mappingFile);
        return ResponseEntity.ok(response);
    }
}