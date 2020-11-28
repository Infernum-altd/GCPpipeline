package com.altynnikov.GCPPipipeline;

import com.altynnikov.GCPPipipeline.example.gcp.Client;
import com.altynnikov.GCPPipipeline.services.BucketService;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.event.annotation.BeforeTestClass;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class BucketServiceTests {
    @Autowired
    private BucketService bucketService;

    @Value("${projectId}")
    private String projectId;
    @Value("${bucketId}")
    private String bucketId;
    private List<Client> clientsList;

    @BeforeTestClass
    public void setUpClientsList() {
        Client client1 = Client.newBuilder()
                .setId(999)
                .setName("Ivan Tester")
                .setPhone("592-000-1236")
                .setAddress("13 Lucky Street")
                .build();

        Client client2 = Client.newBuilder()
                .setId(998)
                .setName("Bogdan Tester")
                .setPhone("192-101-1231")
                .setAddress("13 Unlucky Street")
                .build();

        clientsList = new ArrayList<>();
        clientsList.add(client1);
        clientsList.add(client2);
    }

    @Test
    public void downloadClientFileFromBucketTest() throws Exception {

        bucketService.downloadClientFileFromBucket(projectId, bucketId, "testfile.avsc");

        File expected = new File("src/test/resources/testfile.avsc");
        File actual = new File("src/downloads/testfile.avsc");
        assertTrue(FileUtils.contentEquals(expected, actual), "The files differ!");
    }

    @Test
    public void getClientsFromAvroTest() throws Exception {
        List<Client> actual = bucketService.getClientsFromAvro(new File("src/test/resources/testfile.avsc"));
        assertEquals(clientsList, actual);
    }
}