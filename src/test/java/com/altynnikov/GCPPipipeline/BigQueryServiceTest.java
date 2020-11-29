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
                .setId(999)
                .setName("Ivan Tester")
                .setPhone("592-000-1236")
                .setAddress("13 Lucky Street")
                .build();


        Map<String, Object> rowContent = new HashMap<>();
        rowContent.put("id", client.getId());
        rowContent.put("name", client.getName());
        rowContents.add(rowContent);
    }

    @Test
    public void insetRowsToStorage() throws Exception {
        Logger logger = Logger.getLogger(BigQueryService.class.getName());
        LogHandler handler = new LogHandler();
        handler.setLevel(Level.ALL);
        logger.setUseParentHandlers(true);
        logger.addHandler(handler);
        logger.setLevel(Level.ALL);

        bigQueryService.insetRowsToStorage("clients", "non_optional", rowContents);

        assertEquals(Level.INFO, handler.checkLevel());
    }
}
