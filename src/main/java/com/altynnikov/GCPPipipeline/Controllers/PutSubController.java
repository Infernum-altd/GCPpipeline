package com.altynnikov.GCPPipipeline.Controllers;

import com.altynnikov.GCPPipipeline.Models.Body;
import com.altynnikov.GCPPipipeline.Services.BucketService;
import com.altynnikov.GCPPipipeline.Threads.InsertQueryThread;
import com.altynnikov.GCPPipipeline.example.gcp.Client;
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

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@RestController
public class PutSubController {
    private final Logger log = Logger.getLogger(PutSubController.class.getName());
    private final BucketService bucketService;

    @Value("${projectId}")
    private String projectId;
    @Value("${bucketId}")
    private String bucketId;

    @Autowired
    public PutSubController(BucketService bucketService) {
        this.bucketService = bucketService;
    }

    @RequestMapping(value = "/receivemsg", method = RequestMethod.POST)
    public ResponseEntity receiveMessage(@RequestBody Body body) {
        // Get PubSub message from request body.
        Body.Message message = body.getMessage();

        if (message == null) {
            String msg = "Bad Request: invalid Pub/Sub message format";
            log.log(Level.WARNING, msg);
            return new ResponseEntity(msg, HttpStatus.BAD_REQUEST);
        }

        String data = message.getData();
        String target =
                !StringUtils.isEmpty(data) ? new String(Base64.getDecoder().decode(data)) : "";

        if (target.isEmpty()) {
            String msg = "Bad Request: invalid Pub/Sub message format";
            log.log(Level.WARNING, msg);
            return new ResponseEntity(msg, HttpStatus.BAD_REQUEST);
        }


        Client client = bucketService.
                downloadClientFileFromBucket(projectId, bucketId, new JSONObject(target).getString("name"));

        insertClientData(client);

        return new ResponseEntity(HttpStatus.OK);
    }

    private void insertClientData(Client client) {
        Map<String, Object> nonOptionalRowContent = new HashMap<>();
        nonOptionalRowContent.put("id", client.getId());
        nonOptionalRowContent.put("name", client.getName());

        new Thread( new InsertQueryThread(
                nonOptionalRowContent, "clients", "non_optional")).start();

        Map<String, Object> allClientRowContent = new HashMap<>(nonOptionalRowContent);

        allClientRowContent.put("phone", client.getPhone());
        allClientRowContent.put("address", client.getAddress());

        new Thread( new InsertQueryThread(
                allClientRowContent, "clients", "all_fields")).start();

    }
}
