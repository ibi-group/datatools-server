package com.conveyal.datatools.manager.extensions.mtc;

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by demory on 3/30/16.
 */
public class MtcFeedResource implements ExternalFeedResource {

    public static final Logger LOG = LoggerFactory.getLogger(MtcFeedResource.class);

    private String rtdApi, s3Bucket, s3Prefix, s3CredentialsFilename;

    public MtcFeedResource() {
        rtdApi = DataManager.getConfigPropertyAsText("extensions.mtc.rtd_api");
        s3Bucket = DataManager.getConfigPropertyAsText("extensions.mtc.s3_bucket");
        s3Prefix = DataManager.getConfigPropertyAsText("extensions.mtc.s3_prefix");
        //s3CredentialsFilename = DataManager.config.retrieveById("extensions").retrieveById("mtc").retrieveById("s3_credentials_file").asText();
    }

    @Override
    public String getResourceType() {
        return "MTC";
    }

    @Override
    public void importFeedsForProject(Project project, String authHeader) {
        URL url;
        ObjectMapper mapper = new ObjectMapper();
        // single list from MTC
        try {
            url = new URL(rtdApi + "/Carrier");
        } catch(MalformedURLException ex) {
            LOG.error("Could not construct URL for RTD API: " + rtdApi);
            return;
        }

        try {
            HttpURLConnection con = (HttpURLConnection) url.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", "User-Agent");

            // add auth header
            System.out.println("authHeader="+authHeader);
            con.setRequestProperty("Authorization", authHeader);

            int responseCode = con.getResponseCode();
            System.out.println("\nSending 'GET' request to URL : " + url);
            System.out.println("Response Code : " + responseCode);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            String json = response.toString();
            RtdCarrier[] results = mapper.readValue(json, RtdCarrier[].class);
            for (int i = 0; i < results.length; i++) {
                //                    String className = "RtdCarrier";
                //                    Object car = Class.forName(className).newInstance();
                RtdCarrier car = results[i];
                //System.out.println("car id=" + car.AgencyId + " name=" + car.AgencyName);

                FeedSource source = null;

                // check if a FeedSource with this AgencyId already exists
                for (FeedSource existingSource : project.retrieveProjectFeedSources()) {
                    ExternalFeedSourceProperty agencyIdProp =
                            ExternalFeedSourceProperty.find(existingSource, this.getResourceType(), "AgencyId");
                    if (agencyIdProp != null && agencyIdProp.value != null && agencyIdProp.value.equals(car.AgencyId)) {
                        //System.out.println("already exists: " + car.AgencyId);
                        source = existingSource;
                    }
                }

                String feedName;
                if (car.AgencyName != null) {
                    feedName = car.AgencyName;
                } else if (car.AgencyShortName != null) {
                    feedName = car.AgencyShortName;
                } else {
                    feedName = car.AgencyId;
                }

                if (source == null) {
                    source = new FeedSource(feedName);
                }
                else source.name = feedName;

                source.projectId = project.id;

                source.save();

                // create / update the properties

                for(Field carrierField : car.getClass().getDeclaredFields()) {
                    String fieldName = carrierField.getName();
                    String fieldValue = carrierField.get(car) != null ? carrierField.get(car).toString() : null;
                    ExternalFeedSourceProperty.updateOrCreate(source, this.getResourceType(), fieldName, fieldValue);
                }
            }
        } catch(Exception ex) {
            LOG.error("Could not read feeds from MTC RTD API");
            ex.printStackTrace();
        }
    }

    @Override
    public void feedSourceCreated(FeedSource source, String authHeader) {
        LOG.info("Processing new FeedSource " + source.name + " for RTD");

        RtdCarrier carrier = new RtdCarrier();
        carrier.AgencyName = source.name;

        try {
            for (Field carrierField : carrier.getClass().getDeclaredFields()) {
                String fieldName = carrierField.getName();
                String fieldValue = carrierField.get(carrier) != null ? carrierField.get(carrier).toString() : null;
                ExternalFeedSourceProperty.updateOrCreate(source, this.getResourceType(), fieldName, fieldValue);
            }
        } catch (Exception e) {
            LOG.error("Error creating external properties for new FeedSource");
        }
    }

    @Override
    public void propertyUpdated(ExternalFeedSourceProperty property, String previousValue, String authHeader) {
        LOG.info("Update property in MTC carrier table: " + property.name);

        // sync w/ RTD
        RtdCarrier carrier = new RtdCarrier();
        String feedSourceId = property.feedSourceId;
        FeedSource source = Persistence.feedSources.getById(feedSourceId);

        carrier.AgencyId = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyId").value;
        carrier.AgencyPhone = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyPhone").value;
        carrier.AgencyName = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyName").value;
        carrier.RttAgencyName = ExternalFeedSourceProperty.find(source, this.getResourceType(), "RttAgencyName").value;
        carrier.RttEnabled = ExternalFeedSourceProperty.find(source, this.getResourceType(), "RttEnabled").value;
        carrier.AgencyShortName = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyShortName").value;
        carrier.AgencyPublicId = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyPublicId").value;
        carrier.AddressLat = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AddressLat").value;
        carrier.AddressLon = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AddressLon").value;
        carrier.DefaultRouteType = ExternalFeedSourceProperty.find(source, this.getResourceType(), "DefaultRouteType").value;
        carrier.CarrierStatus = ExternalFeedSourceProperty.find(source, this.getResourceType(), "CarrierStatus").value;
        carrier.AgencyAddress = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyAddress").value;
        carrier.AgencyEmail = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyEmail").value;
        carrier.AgencyUrl = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyUrl").value;
        carrier.AgencyFareUrl = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyFareUrl").value;

        if(property.name.equals("AgencyId") && previousValue == null) {
            writeCarrierToRtd(carrier, true, authHeader);
        }
        else {
            writeCarrierToRtd(carrier, false, authHeader);
        }
    }

    @Override
    public void feedVersionCreated(FeedVersion feedVersion, String authHeader) {

        LOG.info("Pushing to MTC S3 Bucket " + s3Bucket);

        if(s3Bucket == null) return;

        ExternalFeedSourceProperty agencyIdProp =
                ExternalFeedSourceProperty.find(feedVersion.parentFeedSource(), this.getResourceType(), "AgencyId");

        if(agencyIdProp == null || agencyIdProp.equals("null")) {
            LOG.error("Could not read AgencyId for FeedSource " + feedVersion.feedSourceId);
            return;
        }

        String keyName = this.s3Prefix + agencyIdProp.value + ".zip";
        LOG.info("Pushing to MTC S3 Bucket: " + keyName);

        File file = feedVersion.retrieveGtfsFile();

        FeedStore.s3Client.putObject(new PutObjectRequest(
                s3Bucket, keyName, file));
    }

    private void writeCarrierToRtd(RtdCarrier carrier, boolean createNew, String authHeader) {

        try {
            ObjectMapper mapper = new ObjectMapper();

            String carrierJson = mapper.writeValueAsString(carrier);

            URL rtdUrl = new URL(rtdApi + "/Carrier/" + (createNew ? "" : carrier.AgencyId));
            LOG.info("Writing to RTD URL: " + rtdUrl);
            HttpURLConnection connection = (HttpURLConnection) rtdUrl.openConnection();

            connection.setRequestMethod(createNew ? "POST" : "PUT");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Authorization", authHeader);

            OutputStreamWriter osw = new OutputStreamWriter(connection.getOutputStream());
            osw.write(carrierJson);
            osw.flush();
            osw.close();
            LOG.info("RTD API response: " + connection.getResponseCode() + " / " + connection.getResponseMessage());
        } catch (Exception e) {
            LOG.error("error writing to RTD");
            e.printStackTrace();
        }
    }
}
