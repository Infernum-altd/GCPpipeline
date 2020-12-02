package com.altynnikov.GCPPipipeline;

import com.altynnikov.GCPPipipeline.example.gcp.Client;
import com.altynnikov.GCPPipipeline.exeptions.ResponseHasErrorsException;
import com.altynnikov.GCPPipipeline.helpers.BigQueryUtils;
import com.altynnikov.GCPPipipeline.helpers.GetClientList;
import com.altynnikov.GCPPipipeline.helpers.LogHandler;
import com.altynnikov.GCPPipipeline.services.BigQueryService;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
public class BigQueryServiceTest {
    @Autowired
    private BigQueryService bigQueryService;
    @Mock
    private BigQueryService bigQueryServiceMock;
    private static List<Client> clients = new ArrayList<>();
    private static List<Client> damagedClients = new ArrayList<>();
    private static final String projectId = "splendid-tower-297314";
    private static BigQuery bigquery;
    private static final String dataset = "test";
    private static final String jsonKeyPath = "/splendid-tower-297314-eb167dbe4da0.json";

    @BeforeAll
    public static void setUp() throws Exception {
        clients = GetClientList.clientsList;

        RemoteBigQueryHelper bigqueryHelper = RemoteBigQueryHelper.create(projectId, new FileInputStream(
                new File("src/main/resources/splendid-tower-297314-eb167dbe4da0.json").getAbsolutePath()));
        bigquery = bigqueryHelper.getOptions().getService();

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

        bigQueryService.insertClientDataSync(clients, dataset, jsonKeyPath);

        assertEquals(Level.INFO, handler.checkLevel());
    }

    @Test
    public void insertClientDataSyncTestMocked() throws Exception {
        Mockito.doNothing().when(bigQueryServiceMock).insertClientDataSync(clients, dataset, jsonKeyPath);
        bigQueryServiceMock.insertClientDataSync(clients, dataset, jsonKeyPath);

        Mockito.verify(bigQueryServiceMock, Mockito.times(1)).insertClientDataSync(clients, dataset, jsonKeyPath);
    }

    @Test
    public void insertClientDataSyncTestIOException() throws Exception {
        Mockito.doThrow(IOException.class).when(bigQueryServiceMock).insertClientDataSync(clients, dataset, "");

        assertThrows(IOException.class, () -> bigQueryServiceMock.insertClientDataSync(clients, dataset, ""));
    }

    @Test
    public void insertClientDataSyncTestResponseHasErrorsException() throws Exception {
        Mockito.doThrow(ResponseHasErrorsException.class).when(bigQueryServiceMock).insertClientDataSync(clients, "error", jsonKeyPath);

        assertThrows(ResponseHasErrorsException.class, () -> bigQueryServiceMock.insertClientDataSync(clients, "error", jsonKeyPath));
    }


}
