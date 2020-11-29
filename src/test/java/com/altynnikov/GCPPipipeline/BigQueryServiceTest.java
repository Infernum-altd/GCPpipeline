package com.altynnikov.GCPPipipeline;

import com.altynnikov.GCPPipipeline.example.gcp.Client;
import com.altynnikov.GCPPipipeline.helpers.LogHandler;
import com.altynnikov.GCPPipipeline.services.BigQueryService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;



import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class BigQueryServiceTest {
    @Autowired
    private BigQueryService bigQueryService;
    private static final List<Map<String, Object>> rowContents = new ArrayList<>();

    @BeforeAll
    public static void setUp() {
        Client client = Client.newBuilder()
                .setId(998)
                .setName("Ivan Tester")
                .setPhone("592-000-1236")
                .setAddress("13 Lucky Street")
                .build();

        Client client2 = Client.newBuilder()
                .setId(997)
                .setName("Bogdan Tester")
                .setPhone("192-101-1231")
                .setAddress("12 Unlucky Street")
                .build();


        Map<String, Object> rowContent = new HashMap<>();
        rowContent.put("id", client.getId());
        rowContent.put("name", client.getName());
        rowContent.put("phone", client.getPhone());
        rowContent.put("address", client.getAddress());
        rowContents.add(rowContent);

        Map<String, Object> rowContent2 = new HashMap<>();
        rowContent2.put("id", client2.getId());
        rowContent2.put("name", client2.getName());
        rowContent2.put("phone", client2.getPhone());
        rowContent2.put("address", client2.getAddress());
        rowContents.add(rowContent2);
    }

    @Test
    public void insetRowsToStorage() throws Exception {
        Logger logger = Logger.getLogger(BigQueryService.class.getName());
        LogHandler handler = new LogHandler();
        handler.setLevel(Level.ALL);
        logger.setUseParentHandlers(true);
        logger.addHandler(handler);
        logger.setLevel(Level.ALL);

        bigQueryService.insetRowsToStorage("clients", "all_fields", rowContents);

        assertEquals(Level.INFO, handler.checkLevel());
    }
}
