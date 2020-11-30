package com.altynnikov.GCPPipipeline.controllers;

import com.altynnikov.GCPPipipeline.exeptions.ResponseHasErrorsException;
import com.altynnikov.GCPPipipeline.models.Body;
import com.altynnikov.GCPPipipeline.services.BigQueryService;
import com.altynnikov.GCPPipipeline.services.BucketService;
import com.altynnikov.GCPPipipeline.example.gcp.Client;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

@RestController
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class PutSubController {
    @Value("${projectId}")
    private String projectId;
    @Value("${bucketId}")
    private String bucketId;

    private final Logger LOG = Logger.getLogger(PutSubController.class.getName());

    private final BucketService bucketService;
    private final BigQueryService bigQueryService;

    @RequestMapping(value = "/receivemsg", method = RequestMethod.POST)
    public ResponseEntity<String> receiveMessage(@RequestBody Body body) throws IOException, ResponseHasErrorsException {
        System.out.println(body);
        // Get PubSub message from request body.
        Body.Message message = body.getMessage();

        if (message == null || StringUtils.isEmpty(message.getData())) {
            String msg = "Bad Request: invalid Pub/Sub message format";
            LOG.log(Level.WARNING, msg);
            return new ResponseEntity<>(msg, HttpStatus.BAD_REQUEST);
        }

        String target = new String(Base64.getDecoder().decode(message.getData()));

        byte[] downloadedAvro = bucketService.
                downloadClientFileFromBucket(projectId, bucketId, new JSONObject(target).getString("name"));

        List<Client> clients = bucketService.getClientsFromAvro(downloadedAvro);

        insertClientDataSync(clients);

        return new ResponseEntity<>(HttpStatus.OK);
    }

    private void insertClientDataSync(List<Client> clients) throws IOException {
        List<Map<String, Object>> rowContentForAllFields = new ArrayList<>();

        for (Client client : clients) {
            Map<String, Object> rowContent = new HashMap<>();
            rowContent.put("id", client.getId());
            rowContent.put("name", client.getName().toString());
            rowContent.put("phone", client.getPhone().toString());
            rowContent.put("address", client.getAddress().toString());
            rowContentForAllFields.add(rowContent);
        }

        bigQueryService.insetRowsToStorage("clients", "all_fields", rowContentForAllFields);

        List<Map<String, Object>> rowContents = new ArrayList<>();

        for (Client client : clients) {
            Map<String, Object> rowContent = new HashMap<>();
            rowContent.put("id", client.getId());
            rowContent.put("name", client.getName().toString());
            rowContents.add(rowContent);
        }

        bigQueryService.insetRowsToStorage("clients", "non_optional", rowContents);
    }
}
