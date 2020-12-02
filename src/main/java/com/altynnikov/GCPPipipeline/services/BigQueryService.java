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

    public void insertClientDataSync(List<Client> clients, String dataSetName, String jsonKeyPath) throws IOException, ResponseHasErrorsException {
        insetRowsToStorage(dataSetName, "all_fields", ClientUtils.getOptionalRowContent(clients), jsonKeyPath);

        insetRowsToStorage(dataSetName, "non_optional", ClientUtils.getNonOptionalRowContent(clients), jsonKeyPath);
    }

    private void insetRowsToStorage(String dataSetName, String tableName, List<Map<String, Object>> rowContents, String jsonKeyPath) throws IOException, ResponseHasErrorsException {
        List<InsertAllRequest.RowToInsert> rowToInserts = new ArrayList<>();
        for (Map<String, Object> rowContent : rowContents) {
            rowToInserts.add(InsertAllRequest.RowToInsert.of(rowContent));
        }

        insetRowToStorage(dataSetName, tableName, rowToInserts, jsonKeyPath);
    }

    private void insetRowToStorage(String dataSetName, String tableName, List<InsertAllRequest.RowToInsert> rowContents, String jsonKeyPath) throws ResponseHasErrorsException, IOException, BigQueryException {
        bigQuery = (bigQuery == null) ? BigQueryOptions.newBuilder()
                .setCredentials(googleCredentialsService.getCredentials(jsonKeyPath)).build().getService() : bigQuery;

        // Get table
        TableId tableId = TableId.of(dataSetName, tableName);

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
