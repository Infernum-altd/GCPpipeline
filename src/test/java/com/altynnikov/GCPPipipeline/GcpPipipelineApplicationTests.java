package com.altynnikov.GCPPipipeline;

import com.altynnikov.GCPPipipeline.services.BucketService;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@AutoConfigureMockMvc
class GcpPipelineApplicationTests {
    @Autowired
    private BucketService bucketService;

    @Value("${projectId}")
    private String projectId;
    @Value("${bucketId}")
    private String bucketId;

    @Test
    public void downloadClientFileFromBucketTest() throws Exception {

        bucketService.downloadClientFileFromBucket(projectId, bucketId, "testfile.avsc");

        File expected = new File("src/test/resources/testfile.avsc");
        File actual = new File("src/downloads/testfile.avsc");
        assertTrue(FileUtils.contentEquals(expected, actual), "The files differ!");
    }
}
