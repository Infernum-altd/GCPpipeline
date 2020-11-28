package com.altynnikov.GCPPipipeline.services;

import com.altynnikov.GCPPipipeline.exeptions.ResponseHasErrorsException;
import com.google.cloud.bigquery.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@Service
//@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class BigQueryService {
    private final Logger LOG = Logger.getLogger(BigQueryService.class.getName());

    private final GoogleCredentialsService googleCredentialsService;

    @Autowired
    public BigQueryService(GoogleCredentialsService googleCredentialsService) {
        this.googleCredentialsService = googleCredentialsService;
    }

    public void insetRowsToStorage(String datasetName, String tableName, List<Map<String, Object>> rowContents) throws IOException {
        for (Map<String, Object> rowContent : rowContents) {
            try {
                insetRowToStorage(datasetName, tableName, rowContent);
            } catch (ResponseHasErrorsException e) {
                LOG.log(Level.WARNING, e.getMessage());
            }
        }
    }

    private void insetRowToStorage(String datasetName, String tableName, Map<String, Object> rowContent) throws ResponseHasErrorsException, IOException {
        try {
            BigQuery BIG_QUERY = BigQueryOptions.newBuilder()
                    .setCredentials(googleCredentialsService.getCredentials()).build().getService();
            // Get table
            TableId tableId = TableId.of(datasetName, tableName);

            // Inserts rowContent into datasetName:tableId.
            InsertAllResponse response =
                    BIG_QUERY.insertAll(
                            InsertAllRequest.newBuilder(tableId)
                                    .addRow(rowContent)
                                    .build());

            if (response.hasErrors()) {
                // If any of the insertions failed, throws Exception
                StringBuilder errMessages = new StringBuilder();
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    errMessages.append("Response error: ").append(entry.getValue());
                }
                throw new ResponseHasErrorsException(errMessages.toString());
            }

            LOG.log(Level.INFO, "Rows successfully inserted into table");
        } catch (BigQueryException e) {
            LOG.log(Level.WARNING, "Insert operation not performed " + e.toString());
        }
    }
}
