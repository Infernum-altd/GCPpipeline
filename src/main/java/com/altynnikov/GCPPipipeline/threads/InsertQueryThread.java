package com.altynnikov.GCPPipipeline.threads;

import com.altynnikov.GCPPipipeline.services.BigQueryService;

import java.util.List;
import java.util.Map;

public class InsertQueryThread implements Runnable {
    private final List<Map<String, Object>> rowContents;
    private final String datasetName;
    private final String tableName;

    public InsertQueryThread(List<Map<String, Object>> rowContents, String datasetName, String tableName) {
        this.rowContents = rowContents;
        this.datasetName = datasetName;
        this.tableName = tableName;
    }

    @Override
    public void run() {
        //new BigQueryService().insetRowsToStorage(datasetName, tableName, rowContents);
    }
}
