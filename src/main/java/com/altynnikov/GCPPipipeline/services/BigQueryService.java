package com.altynnikov.GCPPipipeline.services;

import com.altynnikov.GCPPipipeline.example.gcp.Client;
import com.altynnikov.GCPPipipeline.exeptions.ResponseHasErrorsException;
import com.google.cloud.bigquery.*;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class BigQueryService {
    private final Logger LOG = Logger.getLogger(BigQueryService.class.getName());
    private final GoogleCredentialsService googleCredentialsService;
    private BigQuery bigQuery = null;

    public void insetRowsToStorage(String datasetName, String tableName, List<Map<String, Object>> rowContents) throws IOException {
        List<InsertAllRequest.RowToInsert> rowToInserts = new ArrayList<>();
        for (Map<String, Object> rowContent : rowContents) {
            rowToInserts.add(InsertAllRequest.RowToInsert.of(rowContent));
        }

        try {
            insetRowToStorage(datasetName, tableName, rowToInserts);
        } catch (ResponseHasErrorsException e) {
            LOG.log(Level.WARNING, e.getMessage());
        }
    }

    public void insertClientDataSync(List<Client> clients) throws IOException {
        List<Map<String, Object>> rowContentForAllFields = new ArrayList<>();

        for (Client client : clients) {
            Map<String, Object> rowContent = new HashMap<>();
            rowContent.put("id", client.getId());
            rowContent.put("name", client.getName().toString());
            rowContent.put("phone", client.getPhone().toString());
            rowContent.put("address", client.getAddress().toString());
            rowContentForAllFields.add(rowContent);
        }

        insetRowsToStorage("clients", "all_fields", rowContentForAllFields);

        List<Map<String, Object>> rowContents = new ArrayList<>();

        for (Client client : clients) {
            Map<String, Object> rowContent = new HashMap<>();
            rowContent.put("id", client.getId());
            rowContent.put("name", client.getName().toString());
            rowContents.add(rowContent);
        }

        insetRowsToStorage("clients", "non_optional", rowContents);
    }

    private void insetRowToStorage(String datasetName, String tableName, List<InsertAllRequest.RowToInsert> rowContents) throws ResponseHasErrorsException, IOException {
        try {

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
