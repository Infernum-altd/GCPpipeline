package com.altynnikov.GCPPipipeline.services;

import com.google.auth.oauth2.GoogleCredentials;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;

@Service
public class GoogleCredentialsService {
    @Value("${authKeyPath}")
    private String jsonKeyPath;

    public GoogleCredentials getCredentials() throws IOException {

        return GoogleCredentials.fromStream(GoogleCredentialsService.class.getResourceAsStream(jsonKeyPath))
                .createScoped("https://www.googleapis.com/auth/cloud-platform");
    }
}
