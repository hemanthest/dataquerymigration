package com.querymanager.service;

import com.microsoft.playwright.*;
import com.microsoft.playwright.options.AriaRole;
import com.microsoft.playwright.options.LoadState;
import com.microsoft.playwright.options.WaitForSelectorState;
import com.querymanager.dto.QueryDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class ZuoraServiceImpl implements ZuoraService {

    @Value("${zuora.username}")
    private String username;

    @Value("${zuora.password}")
    private String password;

    @Override
    public List<QueryDTO> extractQueries() {
        try (Playwright playwright = Playwright.create()) {
            log.info("Starting Playwright to extract queries");
            Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(false));
            BrowserContext context = browser.newContext();
            Page page = context.newPage();
            page.setDefaultTimeout(60000);

            performLogin(page);

            // Wait for and handle tenant selection popup
            Page page1 = page.waitForPopup(() -> {
                page.getByRole(AriaRole.LINK, new Page.GetByRoleOptions().setName("SBX more_vert Tenant ID:")).click();
            });
            page1.waitForLoadState(LoadState.DOMCONTENTLOADED);
            page1.getByRole(AriaRole.LINK, new Page.GetByRoleOptions().setName("Data Query")).click();
            page1.waitForLoadState(LoadState.DOMCONTENTLOADED);

            // Wait for table to load
            page1.waitForSelector("table.MuiTable-root.css-14cy87u");

            // Set rows per page to maximum
            page1.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("20")).click();
            page1.getByRole(AriaRole.OPTION, new Page.GetByRoleOptions().setName("50")).click();

            // Fetch data with lazy loading
            Locator tableBody = page1.locator("table.MuiTable-root.css-14cy87u tbody.MuiTableBody-root.css-1xnox0e");
            List<QueryDTO> queries = new ArrayList<>();

            int previousCount = 0;
            while (true) {
                int currentCount = tableBody.locator("tr.MuiTableRow-root.css-1rqrgdw").count();
                if (currentCount == previousCount) break;
                previousCount = currentCount;
                tableBody.locator("tr.MuiTableRow-root.css-1rqrgdw").last().scrollIntoViewIfNeeded();
                page1.waitForTimeout(500);
            }

            // Extract row data (queryName, description, originalQuery)
            Locator rows = tableBody.locator("tr.MuiTableRow-root.css-1rqrgdw");
            for (int i = 0; i < rows.count(); i++) {
                Locator row = rows.nth(i);
                List<Locator> cells = row.locator(
                        "td.MuiTableCell-root.MuiTableCell-body.MuiTableCell-alignLeft.MuiTableCell-sizeMedium.css-s92qlh," +
                        "td.MuiTableCell-root.MuiTableCell-body.MuiTableCell-alignLeft.MuiTableCell-sizeMedium.css-zbx8d2"
                ).all();

                if (cells.size() >= 2) {
                    QueryDTO query = new QueryDTO();
                    query.setQueryName(cells.get(0).textContent().trim());
                    query.setDescription(cells.get(1).textContent().trim());
                    try {
                        // The list view also contains a column for the SQL text; capture it as originalQuery
                        // Assuming it is the third column in the same row
                        if (cells.size() >= 3) {
                            String sqlText = cells.get(2).textContent() != null ? cells.get(2).textContent().trim() : "";
                            query.setOriginalQuery(sqlText);
                        }
                    } catch (Exception ignored) {
                        // If the column is not present, leave originalQuery empty
                    }
                    queries.add(query);
                }
            }

            log.info("Successfully extracted {} queries", queries.size());
            return queries;

        } catch (Exception e) {
            log.error("Failed to extract queries", e);
            throw new RuntimeException("Failed to extract queries: " + e.getMessage(), e);
        }
    }

    @Override
    public List<QueryDTO> updateQueries(List<QueryDTO> impactedQueries) {
        try (Playwright playwright = Playwright.create()) {
            log.info("Starting Playwright automation for {} queries", impactedQueries.size());
            Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(false));
            BrowserContext context = browser.newContext();
            Page page = context.newPage();
            page.setDefaultTimeout(60000);

            performLogin(page);

            // Select Tenant and navigate to Data Query
            Page page1 = page.waitForPopup(() -> {
                page.getByRole(AriaRole.LINK, new Page.GetByRoleOptions().setName("SBX more_vert Tenant ID:")).click();
            });
            page1.waitForLoadState(LoadState.DOMCONTENTLOADED);
            page1.getByRole(AriaRole.LINK, new Page.GetByRoleOptions().setName("Data Query")).click();
            page1.waitForLoadState(LoadState.DOMCONTENTLOADED);

            // Process each query
            for (QueryDTO query : impactedQueries) {
                try {
                    updateQuery(page1, query);
                } catch (Exception e) {
                    log.error("Failed to update query: " + query.getQueryName(), e);
                    query.setStatus("Failed: " + e.getMessage() + " ❌");
                }
            }

            log.info("Completed updating {} queries", impactedQueries.size());
            return impactedQueries;
        }
    }

    private void performLogin(Page page) {
        log.info("Logging in to Zuora...");
        page.navigate("https://one.zuora.com/one-id/login");
        page.locator("input[type='text']").fill(username);
        page.locator("#mui-5").fill(password);
        page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Toggle password visibility")).click();
        page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Sign in").setExact(true)).click();
    }

    private void updateQuery(Page page, QueryDTO query) {
        log.info("Processing query: {}", query.getQueryName());

        try {
            // Search for query
            page.getByRole(AriaRole.TEXTBOX, new Page.GetByRoleOptions().setName("Type here to filter results...")).fill(query.getQueryName());
            page.waitForTimeout(2000);
            Locator queryLink = page.getByRole(AriaRole.LINK, new Page.GetByRoleOptions().setName(query.getQueryName()));

            if (queryLink.count() == 0) {
                throw new RuntimeException("Query not found: " + query.getQueryName());
            }

            // Open query and save old URL
            queryLink.first().click();
            page.waitForLoadState(LoadState.DOMCONTENTLOADED);
            String oldUrl = page.url();
            query.setOldUrl(oldUrl);

            // Create new query
            page.getByRole(AriaRole.LINK, new Page.GetByRoleOptions().setName("Create New Data Query").setExact(true)).click();
            page.waitForLoadState(LoadState.DOMCONTENTLOADED);
            Locator editor = page.getByRole(AriaRole.TEXTBOX, new Page.GetByRoleOptions().setName("Editor content;Press Alt+F1"));
            editor.waitFor(new Locator.WaitForOptions().setState(WaitForSelectorState.VISIBLE).setTimeout(60000));
            editor.press("ControlOrMeta+a");
            editor.press("Backspace");
            page.waitForTimeout(1000);
            
            // Check if updated query is available
            String updatedQuery = query.getUpdatedQuery();
            if (updatedQuery == null || updatedQuery.trim().isEmpty()) {
                throw new RuntimeException("Updated query content is missing for query: " + query.getQueryName());
            }
            editor.fill(updatedQuery);

            // Save Query
            page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Save Query")).click();

            // Enter new query name
            String newQueryName = query.getQueryName() + "-New";
            Locator queryNameBox = page.getByRole(AriaRole.TEXTBOX, new Page.GetByRoleOptions().setName("Query Name"));
            queryNameBox.click();
            queryNameBox.fill(newQueryName);
            Locator saveButton = page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName("Save"));
            saveButton.waitFor(new Locator.WaitForOptions().setState(WaitForSelectorState.VISIBLE).setTimeout(20000));
            saveButton.click();

            // Wait and capture new URL
            page.waitForLoadState(LoadState.LOAD);
            page.waitForTimeout(3000);
            String newUrl = page.url();
            query.setNewUrl(newUrl);
            query.setStatus("Success ✅");

            // Return to query list
            String dataQueryUrl = "https://apisandbox.zuora.com/platform/apps/data-query/saved-queries";
            page.navigate(dataQueryUrl);
            page.waitForLoadState(LoadState.DOMCONTENTLOADED);
            page.waitForTimeout(2000);

        } catch (Exception e) {
            log.error("Error updating query: " + query.getQueryName(), e);
            query.setStatus("Failed: " + e.getMessage() + " ❌");
            
            // Always try to return to query list page
            try {
                String dataQueryUrl = "https://apisandbox.zuora.com/platform/apps/data-query/saved-queries";
                page.navigate(dataQueryUrl);
                page.waitForLoadState(LoadState.DOMCONTENTLOADED);
                page.waitForTimeout(2000);
            } catch (Exception navEx) {
                log.error("Failed to return to query list", navEx);
                page.reload();
            }
        }
    }
}