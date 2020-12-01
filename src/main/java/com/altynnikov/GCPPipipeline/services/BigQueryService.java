package com.altynnikov.GCPPipipeline.services;

import com.altynnikov.GCPPipipeline.example.gcp.Client;
import com.altynnikov.GCPPipipeline.exeptions.ResponseHasErrorsException;
import com.altynnikov.GCPPipipeline.util.ClientUtils;
import com.google.cloud.bigquery.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Log
public class BigQueryService {
    private final GoogleCredentialsService googleCredentialsService;
    private BigQuery bigQuery = null;

    public void insetRowsToStorage(String datasetName, String tableName, List<Map<String, Object>> rowContents) throws IOException, ResponseHasErrorsException {
        List<InsertAllRequest.RowToInsert> rowToInserts = new ArrayList<>();
        for (Map<String, Object> rowContent : rowContents) {
            rowToInserts.add(InsertAllRequest.RowToInsert.of(rowContent));
        }

        insetRowToStorage(datasetName, tableName, rowToInserts);
    }

    public void insertClientDataSync(List<Client> clients) throws IOException, ResponseHasErrorsException {
        insetRowsToStorage("clients", "all_fields", ClientUtils.getOptionalRowContent(clients));

        insetRowsToStorage("clients", "non_optional", ClientUtils.getNonOptionalRowContent(clients));
    }

    private void insetRowToStorage(String datasetName, String tableName, List<InsertAllRequest.RowToInsert> rowContents) throws ResponseHasErrorsException, IOException, BigQueryException {
        bigQuery = (bigQuery == null) ? BigQueryOptions.newBuilder()
                .setCredentials(googleCredentialsService.getCredentials()).build().getService() : bigQuery;

        // Get table
        TableId tableId = TableId.of(datasetName, tableName);

        // Inserts rowContent into datasetName:tableId.
        InsertAllResponse response =
                bigQuery.insertAll(
                        InsertAllRequest.newBuilder(tableId)
                                .setRows(rowContents)
                                .build());

        if (response.hasErrors()) {
            // If any of the insertions failed, throws Exception
            throw new ResponseHasErrorsException(buildResponseErrorMessage(response));
        }

        log.log(Level.INFO, "Rows successfully inserted into table");
    }

    private String buildResponseErrorMessage(InsertAllResponse response) {
        StringBuilder errMessages = new StringBuilder();
        for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
            errMessages.append("Response error: ").append(entry.getValue());
        }
        return errMessages.toString();
    }
}
