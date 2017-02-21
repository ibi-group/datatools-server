package com.conveyal.datatools.manager.auth;

import com.conveyal.datatools.manager.DataManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.URL;

import static spark.Spark.halt;

/**
 * Created by demory on 3/22/16.
 */

public class Auth0Connection {
    private static final Logger LOG = LoggerFactory.getLogger(Auth0Connection.class);
    public static void checkUser(Request req) {
        String token = getToken(req);

        if(token == null) {
            halt(401, "Could not find authorization token");
        }

        try {
            Auth0UserProfile profile = getUserProfile(token);
            req.attribute("user", profile);
        }
        catch(Exception e) {
//            e.printStackTrace();
            LOG.warn("Could not verify user", e);
            halt(401, "Could not verify user");
        }
    }

    public static String getToken(Request req) {
        String token = null;

        final String authorizationHeader = req.headers("Authorization");
        if (authorizationHeader == null) return null;

        // check format (Authorization: Bearer [token])
        String[] parts = authorizationHeader.split(" ");
        if (parts.length != 2) return null;

        String scheme = parts[0];
        String credentials = parts[1];

        if (scheme.equals("Bearer")) token = credentials;
        return token;
    }

    public static Auth0UserProfile getUserProfile(String token) throws Exception {

        URL url = new URL("https://" + DataManager.config.get("auth0").get("domain").asText() + "/tokeninfo");
        HttpsURLConnection con = (HttpsURLConnection) url.openConnection();

        //add request header
        con.setRequestMethod("POST");

        String urlParameters = "id_token=" + token;

        // Send post request
        con.setDoOutput(true);
        DataOutputStream wr = new DataOutputStream(con.getOutputStream());
        wr.writeBytes(urlParameters);
        wr.flush();
        wr.close();

        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        ObjectMapper m = new ObjectMapper();
        return m.readValue(response.toString(), Auth0UserProfile.class);
    }
}
