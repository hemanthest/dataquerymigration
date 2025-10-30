package com.querymanager.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MappingEntry {
    private String deprecatedObject;
    private String newObject;

    // Parsed components
    private String deprecatedTable;
    private String deprecatedField;
    private String newTable;
    private String newField;

    public boolean isFieldLevelMapping() {
        return deprecatedField != null && !deprecatedField.isEmpty();
    }

    public boolean isTableLevelMapping() {
        return deprecatedField == null || deprecatedField.isEmpty();
    }
}