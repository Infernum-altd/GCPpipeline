package com.altynnikov.GCPPipipeline.Services;

import com.altynnikov.GCPPipipeline.example.gcp.Client;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

@Service
public class BucketService {

    public Client downloadClientFileFromBucket(
            String projectId, String bucketName, String objectName) {

        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

        Blob blob = storage.get(BlobId.of(bucketName, objectName));

        JSONObject fileContent = new JSONObject(new String(blob.getContent()));


        return Client.newBuilder()
                .setId(fileContent.getInt("id"))
                .setName(fileContent.getString("name"))
                .setPhone(fileContent.getString("phone"))
                .setAddress(fileContent.getString("address"))
                .build();

    }
}
