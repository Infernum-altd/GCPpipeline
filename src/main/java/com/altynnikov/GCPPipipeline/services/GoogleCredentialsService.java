package com.altynnikov.GCPPipipeline.services;

import com.google.auth.oauth2.GoogleCredentials;
import org.springframework.stereotype.Service;

import java.io.*;

@Service
public class GoogleCredentialsService {
    public GoogleCredentials getCredentials(String pathToKey) throws IOException {

        return GoogleCredentials.fromStream(GoogleCredentialsService.class.getResourceAsStream(pathToKey))
                .createScoped("https://www.googleapis.com/auth/cloud-platform");
    }
}
