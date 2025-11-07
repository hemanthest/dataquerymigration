package com.querymanager.service;

import com.querymanager.dto.QueryDTO;
import java.util.List;

public interface ZuoraService {
    /**
     * Extracts queries from Zuora
     * @return List of QueryDTO objects containing query details
     */
    List<QueryDTO> extractQueries();

    /**
     * Updates multiple queries in Zuora with automation
     * @param impactedQueries List of queries to update with their new SQL
     * @return Updated list of queries with status
     */
    List<QueryDTO> updateQueries(List<QueryDTO> impactedQueries);
}