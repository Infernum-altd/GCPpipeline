package com.altynnikov.GCPPipipeline.util;

import com.altynnikov.GCPPipipeline.example.gcp.Client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientUtils {
    public static List<Map<String, Object>> getOptionalRowContent(List<Client> clients) {
        List<Map<String, Object>> rowContentForAllFields = new ArrayList<>();

        for (Client client : clients) {
            Map<String, Object> rowContent = new HashMap<>();
            rowContent.put("id", client.getId());
            rowContent.put("name", client.getName().toString());
            rowContent.put("phone", client.getPhone().toString());
            rowContent.put("address", client.getAddress().toString());
            rowContentForAllFields.add(rowContent);
        }

        return rowContentForAllFields;
    }

    public static List<Map<String, Object>> getNonOptionalRowContent(List<Client> clients) {
        List<Map<String, Object>> NonOptionalRowContents = new ArrayList<>();

        for (Client client : clients) {
            Map<String, Object> rowContent = new HashMap<>();
            rowContent.put("id", client.getId());
            rowContent.put("name", client.getName().toString());
            NonOptionalRowContents.add(rowContent);
        }
        return NonOptionalRowContents;
    }
}
