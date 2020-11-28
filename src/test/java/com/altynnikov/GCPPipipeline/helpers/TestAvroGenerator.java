package com.altynnikov.GCPPipipeline.helpers;

import com.altynnikov.GCPPipipeline.example.gcp.Client;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class TestAvroGenerator {


    public static void main(String[] args) {
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


        DatumWriter<Client> userDatumWriter = new SpecificDatumWriter<>(Client.class);
        DataFileWriter<Client> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        try {
            dataFileWriter.create(client1.getSchema(), new File("src/test/resources/testfile.avsc"));
            dataFileWriter.append(client1);
            dataFileWriter.append(client2);
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
