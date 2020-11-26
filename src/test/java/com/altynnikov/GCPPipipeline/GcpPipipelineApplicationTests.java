package com.altynnikov.GCPPipipeline;

import com.altynnikov.GCPPipipeline.Services.BucketService;
import com.altynnikov.GCPPipipeline.example.gcp.Client;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@AutoConfigureMockMvc
class GcpPipelineApplicationTests {
    @Autowired
    private BucketService bucketService;
/*    @Autowired
    private MockMvc mockMvc;*/

    @Value("${projectId}")
    private String projectId;
    @Value("${bucketId}")
    private String bucketId;

    @Test
    public void downloadClientFileFromBucketTest() throws Exception {

        assertEquals(Client.newBuilder()
                .setId(999)
                .setName("Ivan Tester")
                .setPhone("592-000-1236")
                .setAddress("13 Lucky Street")
                .build(), bucketService.downloadClientFileFromBucket(projectId, bucketId, "testfile.json"));


    }
}
