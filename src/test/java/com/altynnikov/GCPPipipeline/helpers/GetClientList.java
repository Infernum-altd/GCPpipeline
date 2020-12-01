package com.altynnikov.GCPPipipeline.helpers;

import com.altynnikov.GCPPipipeline.example.gcp.Client;

import java.util.ArrayList;
import java.util.List;

public class GetClientList {
    public static final List<Client> clientsList = new ArrayList<>();

    static {
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

        clientsList.add(client1);
        clientsList.add(client2);
    }
}
