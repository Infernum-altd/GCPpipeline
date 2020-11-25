package com.altynnikov.GCPPipipeline.Services;

import com.google.cloud.bigquery.*;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BigQueryService {
    private static final Logger log = Logger.getLogger(BigQueryService.class.getName());

    public static void insetToStorage(String datasetName, String tableName, Map<String, Object> rowContent) {
        try {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            // Get table
            TableId tableId = TableId.of(datasetName, tableName);

            // Inserts rowContent into datasetName:tableId.
            InsertAllResponse response =
                    bigquery.insertAll(
                            InsertAllRequest.newBuilder(tableId)
                                    .addRow(rowContent)
                                    .build());

            if (response.hasErrors()) {
                // If any of the insertions failed, this lets you inspect the errors
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    log.log(Level.WARNING, "Response error: " + entry.getValue());
                }
            }
            log.log(Level.INFO, "Rows successfully inserted into table");
        } catch (
                BigQueryException e) {
            log.log(Level.WARNING,"Insert operation not performed " + e.toString());
        }
    }
}
