package com.altynnikov.GCPPipipeline.services;

import com.altynnikov.GCPPipipeline.example.gcp.Client;
import com.altynnikov.GCPPipipeline.exeptions.AvroNoClientFoundException;
import lombok.RequiredArgsConstructor;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.beans.factory.annotation.Autowired;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class BucketService {

    private final GoogleCredentialsService googleCredentialsService;

    public byte[] downloadClientFileFromBucket(String projectId, String bucketName, String objectName) throws IOException {

        GoogleCredentials googleCredentials = googleCredentialsService.getCredentials();

        Storage storage = StorageOptions.newBuilder()
                .setCredentials(googleCredentials)
                .setProjectId(projectId).build().getService();

        Blob blob = storage.get(BlobId.of(bucketName, objectName));

        return blob.getContent();
    }

    public List<Client> getClientsFromAvro(byte[] avroFile) throws IOException, AvroNoClientFoundException {
        DatumReader<Client> clientDatumReader = new SpecificDatumReader<>(Client.class);

        DataFileReader<Client> dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(avroFile), clientDatumReader);

        List<Client> clients = new ArrayList<>();

        while (dataFileReader.hasNext()) {
            clients.add(dataFileReader.next());
        }

        if (clients.isEmpty()) throw new AvroNoClientFoundException("In avro wasn`t found any clients record");

        return clients;
    }
}
