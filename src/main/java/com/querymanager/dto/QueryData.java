package com.querymanager.dto;

import lombok.Data;

@Data
public class QueryData {
    private String queryName;
    private String queryDescription;
    private String query;
    private String oldUrl;
    private String newUrl;
    private String status;
}