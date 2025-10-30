package com.querymanager.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MigrationResponse {
    private boolean success;
    private String message;
    private int totalQueries;
    private int impactedQueries;
    private String reportFilePath;
    private String error;
}
