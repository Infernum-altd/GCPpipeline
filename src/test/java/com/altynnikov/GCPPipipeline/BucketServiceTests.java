package com.altynnikov.GCPPipipeline;

import com.altynnikov.GCPPipipeline.example.gcp.Client;
import com.altynnikov.GCPPipipeline.helpers.GetClientList;
import com.altynnikov.GCPPipipeline.services.BucketService;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;


@SpringBootTest
public class BucketServiceTests {
    @Mock
    private BucketService bucketServiceMock;

    private final String projectId = "splendid-tower-297314";
    private final String bucketId = "test_gcppipeline";
    private static List<Client> clientsList = new ArrayList<>();

    @BeforeAll
    public static void setUpClientsList() {
        clientsList = GetClientList.clientsList;
    }

    @Test
    public void downloadClientFileFromBucketTest() throws Exception {
        Mockito.when(bucketServiceMock.downloadClientFileFromBucket(projectId, bucketId, "testfile.avsc", "/splendid-tower-297314-eb167dbe4da0.json"))
                .thenReturn(FileUtils.readFileToByteArray(new File("src/test/resources/testfile.avsc")));

         byte[] actual = bucketServiceMock.downloadClientFileFromBucket(projectId, bucketId, "testfile.avsc", "/splendid-tower-297314-eb167dbe4da0.json");

         assertArrayEquals(FileUtils.readFileToByteArray(new File("src/test/resources/testfile.avsc")), actual);
    }

    @Test
    public void getClientsFromAvroTest() throws Exception {
        Mockito.when(bucketServiceMock.getClientsFromAvro(FileUtils.readFileToByteArray(new File("src/test/resources/testfile.avsc"))))
                .thenReturn(clientsList);
        assertEquals(clientsList, bucketServiceMock
                .getClientsFromAvro(FileUtils.readFileToByteArray(new File("src/test/resources/testfile.avsc"))));
    }
}
