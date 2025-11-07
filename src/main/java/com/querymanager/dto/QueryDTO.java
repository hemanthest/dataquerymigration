package com.querymanager.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class QueryDTO {
    private String queryName;
    private String description;
    private String originalQuery;
    private String updatedQuery;
    private boolean impacted;
    private String oldUrl;
    private String newUrl;
    private String status;
}