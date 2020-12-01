package com.altynnikov.GCPPipipeline.controllers;

import com.altynnikov.GCPPipipeline.exeptions.AvroNoClientFoundException;
import com.altynnikov.GCPPipipeline.exeptions.ResponseHasErrorsException;
import com.altynnikov.GCPPipipeline.models.Body;
import com.altynnikov.GCPPipipeline.services.BigQueryService;
import com.altynnikov.GCPPipipeline.services.BucketService;
import com.altynnikov.GCPPipipeline.example.gcp.Client;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
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
@Log
public class PutSubController {
    @Value("${projectId}")
    private String projectId;
    @Value("${bucketId}")
    private String bucketId;

    private final BucketService bucketService;
    private final BigQueryService bigQueryService;

    @RequestMapping(value = "/receivemsg", method = RequestMethod.POST)
    public ResponseEntity<String> receiveMessage(@RequestBody Body body) {
        // Get PubSub message from request body.
        Body.Message message = body.getMessage();

        if (body.isMessageValid()) {
            String msg = "Bad Request: invalid Pub/Sub message format";
            log.log(Level.WARNING, msg);
            return new ResponseEntity<>(msg, HttpStatus.BAD_REQUEST);
        }

        String target = new String(Base64.getDecoder().decode(message.getData()));

        try {
            byte[] downloadedAvro = bucketService.
                    downloadClientFileFromBucket(projectId, bucketId, new JSONObject(target).getString("name"));

            List<Client> clients = bucketService.getClientsFromAvro(downloadedAvro);

            bigQueryService.insertClientDataSync(clients);
        } catch (IOException | AvroNoClientFoundException | ResponseHasErrorsException e) {
            log.log(Level.SEVERE, e.getMessage());
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(HttpStatus.OK);
    }

}
