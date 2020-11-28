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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Service
public class BucketService {

    @Value("${authKeyPath}")
    private String jsonKeyPath;
    @Value("${downloadPath}")
    private String downloadPath;

    public File downloadClientFileFromBucket(
            String projectId, String bucketName, String objectName) throws IOException {

        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonKeyPath))
                .createScoped("https://www.googleapis.com/auth/cloud-platform");

        Storage storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId(projectId).build().getService();

        Blob blob = storage.get(BlobId.of(bucketName, objectName));
        File file = new File(downloadPath + objectName);

        blob.downloadTo(Paths.get(file.getAbsolutePath()));

        return file;
    }

    public List<Client> getClientsFromAvro(File avroFile) throws IOException{
        DatumReader<Client> clientDatumReader = new SpecificDatumReader<>(Client.class);

        DataFileReader<Client> dataFileReader = new DataFileReader<>(avroFile, clientDatumReader);

        List<Client> clients = new ArrayList<>();

        while (dataFileReader.hasNext()) {
            clients.add(dataFileReader.next());
        }

        return clients;
    }
}
