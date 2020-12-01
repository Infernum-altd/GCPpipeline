package com.altynnikov.GCPPipipeline;

import com.altynnikov.GCPPipipeline.example.gcp.Client;
import com.altynnikov.GCPPipipeline.helpers.BigQueryUtils;
import com.altynnikov.GCPPipipeline.helpers.GetClientList;
import com.altynnikov.GCPPipipeline.helpers.LogHandler;
import com.altynnikov.GCPPipipeline.services.BigQueryService;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;


import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;



import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class BigQueryServiceTest {
    @Autowired
    private BigQueryService bigQueryService;
    private static List<Client> clients = new ArrayList<>();
    private static final String projectId = "splendid-tower-297314";
    private static BigQuery bigquery;
    private static String dataset = "test";

    @BeforeAll
    public static void setUp() throws Exception {
        clients = GetClientList.clientsList;

        RemoteBigQueryHelper bigqueryHelper =
                RemoteBigQueryHelper.create(projectId, new FileInputStream(new File("src/main/resources/splendid-tower-297314-eb167dbe4da0.json").getAbsolutePath()));
        bigquery = bigqueryHelper.getOptions().getService();
        //dataset = RemoteBigQueryHelper.generateDatasetName();

        BigQueryUtils.createTablesForClientTest(dataset);
    }

    @AfterAll
    public static void cleanUp() {
        RemoteBigQueryHelper.forceDelete(bigquery, dataset);
    }

    @Test
    public void insertClientDataSyncTest() throws Exception {
        Logger logger = Logger.getLogger(BigQueryService.class.getName());
        LogHandler handler = new LogHandler();
        handler.setLevel(Level.ALL);
        logger.setUseParentHandlers(true);
        logger.addHandler(handler);
        logger.setLevel(Level.ALL);

        bigQueryService.insertClientDataSync(clients, dataset, "/splendid-tower-297314-eb167dbe4da0.json");

        assertEquals(Level.INFO, handler.checkLevel());
    }
}
