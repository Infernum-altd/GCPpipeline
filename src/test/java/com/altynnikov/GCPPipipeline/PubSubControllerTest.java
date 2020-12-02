package com.altynnikov.GCPPipipeline;

import com.altynnikov.GCPPipipeline.models.Body;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;


import java.util.Base64;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@SpringBootTest
@AutoConfigureMockMvc
public class PubSubControllerTest {

    @Autowired
    private MockMvc mockMvc;

    private String messageInBody = "{\"kind\": \"storage#object\",\n" +
            "  \"id\": \"avro-files-storage/severalClients.avsc/1606733502215887\",\n" +
            "  \"selfLink\": \"https://www.googleapis.com/storage/v1/b/avro-files-storage/o/severalClients.avsc\",\n" +
            "  \"name\":\"testfile.avsc\"}";

    private String messageInBodyEmpty = "{\"kind\": \"storage#object\",\n" +
            "  \"id\": \"avro-files-storage/severalClients.avsc/1606733502215887\",\n" +
            "  \"selfLink\": \"https://www.googleapis.com/storage/v1/b/avro-files-storage/o/severalClients.avsc\",\n" +
            "  \"name\":\"testfileEmpty.avsc\"}";

    @Test
    public void receiveMessageTestOK() throws Exception {

        Body body = new Body();
        body.setMessage(new Body.Message("1", "11:00", Base64.getEncoder().encodeToString(messageInBody.getBytes())));

        ObjectMapper mapper = new ObjectMapper();

        mockMvc.perform(post("/receivemsg")
                .contentType("application/json")
                .content(mapper.writeValueAsString(body)))
                .andExpect(status().is2xxSuccessful());
    }

    @Test
    public void receiveMessageTestServerErrMessageNull() throws Exception {

        Body body = new Body();
        ObjectMapper mapper = new ObjectMapper();

        mockMvc.perform(post("/receivemsg")
        .contentType("application/json")
        .content(mapper.writeValueAsString(body)))
                .andExpect(status().is4xxClientError());
    }

    @Test
    public void receiveMessageTestServerErrMessageDataNull() throws Exception {

        Body body = new Body();
        body.setMessage(new Body.Message("1", "11:00", ""));
        ObjectMapper mapper = new ObjectMapper();

        mockMvc.perform(post("/receivemsg")
                .contentType("application/json")
                .content(mapper.writeValueAsString(body)))
                .andExpect(status().is4xxClientError());
    }

    @Test
    public void receiveMessageTestAvroNoClients() throws Exception {
        Body body = new Body();
        body.setMessage(new Body.Message("1", "11:00", Base64.getEncoder().encodeToString(messageInBodyEmpty.getBytes())));

        ObjectMapper mapper = new ObjectMapper();

        mockMvc.perform(post("/receivemsg")
                .contentType("application/json")
                .content(mapper.writeValueAsString(body)))
                .andExpect(status().is5xxServerError());
    }
}
