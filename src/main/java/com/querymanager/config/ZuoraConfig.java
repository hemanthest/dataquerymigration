package com.querymanager.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "zuora")
@Data
public class ZuoraConfig {
    private String loginUrl;
    private String username;
    private String password;
    private String dataQueryUrl;
    private String tenantId;
    private String outputPath;
    private String inputPath;
}