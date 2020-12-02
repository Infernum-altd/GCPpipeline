package com.altynnikov.GCPPipipeline.helpers;

import com.altynnikov.GCPPipipeline.example.gcp.Client;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestAvroGenerator {
    private final static Logger LOG = Logger.getLogger(TestAvroGenerator.class.getName());
    private final static Client client1 = Client.newBuilder()
            .setId(999)
            .setName("Ivan Tester")
            .setPhone("592-000-1236")
            .setAddress("13 Lucky Street")
            .build();

    public static void main(String[] args) throws IOException, JSONException {
       /* List<Client> clients = parceJsonInClients(Paths.get("src/test/resources/client1.avsc"));

        generateAvro(clients, Paths.get("src/test/resources/severalClients.avsc"));*/

        generateAvro(new ArrayList<>(), Paths.get("src/test/resources/testfileEmpty.avsc"));
    }

    public static void generateAvro(List<Client> clientList, Path targetPath) {
        DatumWriter<Client> userDatumWriter = new SpecificDatumWriter<>(Client.class);

        try (DataFileWriter<Client> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        ) {
            dataFileWriter.create(client1.getSchema(), new File(targetPath.toUri()));
            for (Client client : clientList) {
                dataFileWriter.append(client);
            }
        } catch (IOException e) {
            LOG.log(Level.WARNING, e.getMessage());
        }
    }

    private static List<Client> parceJsonInClients(Path path) throws JSONException, IOException {
        List<Client> result = new ArrayList<>();

        JSONObject jsonObject = new JSONObject(new String(Files.readAllBytes(path)));

        JSONArray jsonArray = jsonObject.getJSONArray("arr");


        for (int i = 0; i < jsonArray.length(); i++) {
            result.add(
                    Client.newBuilder()
                            .setId(((JSONObject)jsonArray.get(i)).getInt("id"))
                            .setName(((JSONObject)jsonArray.get(i)).getString("name"))
                            .setPhone(((JSONObject)jsonArray.get(i)).getString("phone"))
                            .setAddress(((JSONObject)jsonArray.get(i)).getString("address"))
                            .build());

        }
        return result;
    }
}
