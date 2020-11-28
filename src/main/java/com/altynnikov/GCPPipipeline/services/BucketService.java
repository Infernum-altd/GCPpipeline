package com.altynnikov.GCPPipipeline.services;

import com.altynnikov.GCPPipipeline.example.gcp.Client;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.beans.factory.annotation.Value;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class BucketService {

    @Value("${authKeyPath}")
    private String jsonKeyPath;
    @Value("${downloadPath}")
    private String downloadPath;

    public List<Client> downloadClientFileFromBucket(
            String projectId, String bucketName, String objectName) throws IOException {

        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonKeyPath))
                .createScoped("https://www.googleapis.com/auth/cloud-platform");

        Storage storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId(projectId).build().getService();

        Blob blob = storage.get(BlobId.of(bucketName, objectName));
        File file = new File(downloadPath + objectName);

        blob.downloadTo(Paths.get(file.getAbsolutePath()));

/*        JSONObject fileContent = new JSONObject(new String(blob.getContent()));*/

        DatumReader<Client> clientDatumReader = new SpecificDatumReader<>(Client.class);

        DataFileReader<Client> dataFileReader = new DataFileReader<>(file, clientDatumReader);

        List<Client> clients = new ArrayList<>();

        while(dataFileReader.hasNext()){
            clients.add(dataFileReader.next());
        }

        System.out.println(clients.get(0));
        return clients;

/*        return Client.newBuilder()
                .setId(fileContent.getInt("id"))
                .setName(fileContent.getString("name"))
                .setPhone(fileContent.getString("phone"))
                .setAddress(fileContent.getString("address"))
                .build();*/

    }
}
