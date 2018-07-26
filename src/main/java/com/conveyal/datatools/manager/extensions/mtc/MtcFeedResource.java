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

import static com.conveyal.datatools.manager.models.ExternalFeedSourceProperty.constructId;

/**
 * Created by demory on 3/30/16.
 */
public class MtcFeedResource implements ExternalFeedResource {

    public static final Logger LOG = LoggerFactory.getLogger(MtcFeedResource.class);

    private String rtdApi, s3Bucket, s3Prefix, s3CredentialsFilename;

    public static final String AGENCY_ID = "AgencyId";
    public static final String RESOURCE_TYPE = "MTC";
    public MtcFeedResource() {
        rtdApi = DataManager.getExtensionPropertyAsText(RESOURCE_TYPE, "rtd_api");
        s3Bucket = DataManager.getExtensionPropertyAsText(RESOURCE_TYPE, "s3_bucket");
        s3Prefix = DataManager.getExtensionPropertyAsText(RESOURCE_TYPE, "s3_prefix");
        //s3CredentialsFilename = DataManager.config.retrieveById("extensions").retrieveById("mtc").retrieveById("s3_credentials_file").asText();
    }

    @Override
    public String getResourceType() {
        return RESOURCE_TYPE;
    }

    @Override
    public void importFeedsForProject(Project project, String authHeader) {
        URL url;
        ObjectMapper mapper = new ObjectMapper();
        // single list from MTC
        try {
            url = new URL(rtdApi + "/Carrier");
        } catch(MalformedURLException ex) {
            LOG.error("Could not construct URL for RTD API: {}", rtdApi);
            return;
        }

        try {
            HttpURLConnection con = (HttpURLConnection) url.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", "User-Agent");

            // add auth header
            LOG.info("authHeader="+authHeader);
            con.setRequestProperty("Authorization", authHeader);

            int responseCode = con.getResponseCode();
            LOG.info("Sending 'GET' request to URL : " + url);
            LOG.info("Response Code : " + responseCode);

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
                //LOG.info("car id=" + car.AgencyId + " name=" + car.AgencyName);

                FeedSource source = null;

                // check if a FeedSource with this AgencyId already exists
                for (FeedSource existingSource : project.retrieveProjectFeedSources()) {
                    ExternalFeedSourceProperty agencyIdProp;
                    agencyIdProp = Persistence.externalFeedSourceProperties.getById(constructId(existingSource, this.getResourceType(), AGENCY_ID));
                    if (agencyIdProp != null && agencyIdProp.value != null && agencyIdProp.value.equals(car.AgencyId)) {
                        //LOG.info("already exists: " + car.AgencyId);
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
                // Store the feed source.
                Persistence.feedSources.create(source);

                // create / update the properties

                for(Field carrierField : car.getClass().getDeclaredFields()) {
                    String fieldName = carrierField.getName();
                    String fieldValue = carrierField.get(car) != null ? carrierField.get(car).toString() : null;
                    ExternalFeedSourceProperty prop = new ExternalFeedSourceProperty(source, this.getResourceType(), fieldName, fieldValue);
                    if (Persistence.externalFeedSourceProperties.getById(prop.id) == null) {
                        Persistence.externalFeedSourceProperties.create(prop);
                    } else {
                        Persistence.externalFeedSourceProperties.updateField(prop.id, fieldName, fieldValue);
                    }
                }
            }
        } catch(Exception ex) {
            LOG.error("Could not read feeds from MTC RTD API");
            ex.printStackTrace();
        }
    }

    /**
     * Do nothing for now. Creating a new agency for RTD requires adding the AgencyId property (when it was previously
     * null. See {@link #propertyUpdated(ExternalFeedSourceProperty, String, String)}.
     */
    @Override
    public void feedSourceCreated(FeedSource source, String authHeader) {
        LOG.info("Processing new FeedSource {} for RTD. (No action taken.)", source.name);
    }

    /**
     * Sync an updated property with the RTD database. Note: if the property is AgencyId and the value was previously
     * null create/register a new carrier with RTD.
     */
    @Override
    public void propertyUpdated(ExternalFeedSourceProperty updatedProperty, String previousValue, String authHeader) {
        LOG.info("Update property in MTC carrier table: " + updatedProperty.name);
        String feedSourceId = updatedProperty.feedSourceId;
        FeedSource source = Persistence.feedSources.getById(feedSourceId);
        RtdCarrier carrier = new RtdCarrier(source);

        if(updatedProperty.name.equals(AGENCY_ID) && previousValue == null) {
            // If the property being updated is the agency ID field and it previously was null, this indicates that a
            // new carrier should be written to the RTD.
            writeCarrierToRtd(carrier, true, authHeader);
        } else {
            // Otherwise, this is just a standard prop update.
            writeCarrierToRtd(carrier, false, authHeader);
        }
    }

    /**
     * When feed version is created/published, write the feed to the shared S3 bucket.
     */
    @Override
    public void feedVersionCreated(FeedVersion feedVersion, String authHeader) {

        if(s3Bucket == null) {
            LOG.error("Cannot push {} to S3 bucket. No bucket name specified.", feedVersion.id);
            return;
        }
        // Construct agency ID from feed source and retrieve from MongoDB.
        ExternalFeedSourceProperty agencyIdProp = Persistence.externalFeedSourceProperties.getById(
                constructId(feedVersion.parentFeedSource(), this.getResourceType(), AGENCY_ID)
        );

        if(agencyIdProp == null || agencyIdProp.value.equals("null")) {
            LOG.error("Could not read {} for FeedSource {}", AGENCY_ID, feedVersion.feedSourceId);
            return;
        }

        String keyName = String.format("%s%s.zip", this.s3Prefix, agencyIdProp.value);
        LOG.info("Pushing to MTC S3 Bucket: " + keyName);
        File file = feedVersion.retrieveGtfsFile();
        FeedStore.s3Client.putObject(new PutObjectRequest(s3Bucket, keyName, file));
    }

    /**
     * Update or create a carrier and its properties with an HTTP request to the RTD.
     */
    private void writeCarrierToRtd(RtdCarrier carrier, boolean createNew, String authHeader) {

        try {
            ObjectMapper mapper = new ObjectMapper();

            String carrierJson = mapper.writeValueAsString(carrier);

            URL rtdUrl = new URL(rtdApi + "/Carrier/" + (createNew ? "" : carrier.AgencyId));
            LOG.info("Writing to RTD URL: {}", rtdUrl);
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
            LOG.info("RTD API response: {}/{}", connection.getResponseCode(), connection.getResponseMessage());
        } catch (Exception e) {
            LOG.error("Error writing to RTD", e);
        }
    }
}
