package com.altynnikov.GCPPipipeline.helpers;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import lombok.extern.java.Log;

import java.io.IOException;
import java.util.logging.Level;

@Log
public class BigQueryUtils {
    private static BigQuery bigquery = null;

    public static void createTablesForClientTest(String dataSetName) throws IOException {
        createDataset(dataSetName);
        createAllFieldsTable(dataSetName);
        createNonOptionalFieldsTable(dataSetName);
    }

    private static void createDataset(String datasetName) throws IOException {
        try {
            bigquery = (bigquery == null) ? BigQueryOptions.newBuilder()
                    .setCredentials(
                            GoogleCredentials.fromStream(BigQueryUtils.class.getResourceAsStream("/splendid-tower-297314-eb167dbe4da0.json"))
                                    .createScoped("https://www.googleapis.com/auth/cloud-platform")
                    ).build().getService() : bigquery;

            DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();

            Dataset newDataset = bigquery.create(datasetInfo);
            String newDatasetName = newDataset.getDatasetId().getDataset();
            log.log(Level.INFO,newDatasetName + " created successfully");
        } catch (BigQueryException e) {
            log.log(Level.SEVERE, "Dataset was not created. \n" + e.toString());
        }
    }

    private static void createAllFieldsTable(String datasetName) throws IOException {

        String tableName = "all_fields";
        Schema schema =
                Schema.of(
                        Field.of("id", StandardSQLTypeName.INT64),
                        Field.of("name", StandardSQLTypeName.STRING),
                        Field.of("phone", StandardSQLTypeName.STRING),
                        Field.of("address", StandardSQLTypeName.STRING));
        createTable(datasetName, tableName, schema);
    }

    private static void createNonOptionalFieldsTable(String datasetName) throws IOException {

        String tableName = "non_optional";
        Schema schema =
                Schema.of(
                        Field.of("id", StandardSQLTypeName.INT64),
                        Field.of("name", StandardSQLTypeName.STRING),
                        Field.of("phone", StandardSQLTypeName.STRING),
                        Field.of("address", StandardSQLTypeName.STRING));
        createTable(datasetName, tableName, schema);
    }

    private static void createTable(String datasetName, String tableName, Schema schema) throws IOException {
        try {

            bigquery = (bigquery == null) ? BigQueryOptions.newBuilder()
                    .setCredentials(
                            GoogleCredentials.fromStream(BigQueryUtils.class.getResourceAsStream("/splendid-tower-297314-eb167dbe4da0.json"))
                                    .createScoped("https://www.googleapis.com/auth/cloud-platform")
                    ).build().getService() : bigquery;

            TableId tableId = TableId.of(datasetName, tableName);
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

            bigquery.create(tableInfo);
            log.log(Level.INFO, "Table created successfully");
        } catch (BigQueryException e) {
            log.log(Level.SEVERE,"Table was not created. \n" + e.toString());
        }
    }
}
