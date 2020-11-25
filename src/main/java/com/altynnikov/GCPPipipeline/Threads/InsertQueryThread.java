package com.altynnikov.GCPPipipeline.Threads;

import com.altynnikov.GCPPipipeline.Services.BigQueryService;

import java.util.Map;

public class InsertQueryThread implements Runnable {
    private final Map<String, Object> rowContent;
    private final String datasetName;
    private final String tableName;


    public InsertQueryThread(Map<String, Object> rowContent, String datasetName, String tableName) {
        this.rowContent = rowContent;
        this.datasetName = datasetName;
        this.tableName = tableName;
    }

    @Override
    public void run() {
        BigQueryService.insetToStorage(datasetName, tableName, rowContent);
    }
}
