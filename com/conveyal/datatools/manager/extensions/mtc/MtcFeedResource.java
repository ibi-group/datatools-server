package com.conveyal.datatools.manager.extensions.mtc;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
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
        rtdApi = DataManager.config.getProperty("application.extensions.mtc.rtd_api");
        s3Bucket = DataManager.config.getProperty("application.extensions.mtc.s3_bucket");
        s3Prefix = DataManager.config.getProperty("application.extensions.mtc.s3_prefix");
        s3CredentialsFilename = DataManager.config.getProperty("application.extensions.mtc.s3_credentials_file");
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
            System.out.println(json);
            RtdCarrier[] results = mapper.readValue(json, RtdCarrier[].class);
            for (int i = 0; i < results.length; i++) {
                //                    String className = "RtdCarrier";
                //                    Object car = Class.forName(className).newInstance();
                RtdCarrier car = results[i];
                //System.out.println("car id=" + car.AgencyId + " name=" + car.AgencyName);

                FeedSource source = null;

                // check if a FeedSource with this AgencyId already exists
                for (FeedSource existingSource : project.getProjectFeedSources()) {
                    ExternalFeedSourceProperty agencyIdProp =
                            ExternalFeedSourceProperty.find(existingSource, this.getResourceType(), "AgencyId");
                    if (agencyIdProp != null && agencyIdProp.value.equals(car.AgencyId)) {
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

                if (source == null) source = new FeedSource(feedName);
                else source.name = feedName;

                source.setProject(project);

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
    public void propertyUpdated(ExternalFeedSourceProperty property, String authHeader) {
        LOG.info("Update property in MTC carrier table: " + property.name);

        // sync w/ RTD
        RtdCarrier carrier = new RtdCarrier();
        String feedSourceId = property.getFeedSourceId();
        FeedSource source = FeedSource.get(feedSourceId);

        carrier.AgencyId = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyId").value;
        carrier.AgencyPhone = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyPhone").value;
        carrier.AgencyName = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyName").value;
        carrier.RttAgencyName = ExternalFeedSourceProperty.find(source, this.getResourceType(), "RttAgencyName").value;
        carrier.RttEnabled = ExternalFeedSourceProperty.find(source, this.getResourceType(), "RttEnabled").value;
        carrier.AgencyShortName = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyShortName").value;
        carrier.AgencyPublicId = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyPublicId").value;
        carrier.AddressLat = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AddressLon").value;
        carrier.AddressLon = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyPhone").value;
        carrier.DefaultRouteType = ExternalFeedSourceProperty.find(source, this.getResourceType(), "DefaultRouteType").value;
        carrier.CarrierStatus = ExternalFeedSourceProperty.find(source, this.getResourceType(), "CarrierStatus").value;
        carrier.AgencyAddress = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyAddress").value;
        carrier.AgencyEmail = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyEmail").value;
        carrier.AgencyUrl = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyUrl").value;
        carrier.AgencyFareUrl = ExternalFeedSourceProperty.find(source, this.getResourceType(), "AgencyFareUrl").value;

        try {
            ObjectMapper mapper = new ObjectMapper();

            String carrierJson = mapper.writeValueAsString(carrier);

            URL rtdUrl = new URL(rtdApi + "/Carrier/" + carrier.AgencyId);
            LOG.info("Writing to RTD URL: " + rtdUrl);
            HttpURLConnection connection = (HttpURLConnection) rtdUrl.openConnection();

            connection.setRequestMethod("PUT");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Authorization", authHeader);



            OutputStreamWriter osw = new OutputStreamWriter(connection.getOutputStream());
            osw.write(carrierJson);
            osw.flush();
            osw.close();
            LOG.info("RTD API response: " + connection.getResponseCode());
        } catch (Exception e) {
            LOG.error("error writing to RTD");
            e.printStackTrace();
        }
    }

    @Override
    public void feedVersionUpdated(FeedVersion feedVersion, String authHeader) {

        LOG.info("Pushing to MTC S3 Bucket " + s3Bucket);

        if(s3Bucket == null) return;

        AWSCredentials creds;
        if (this.s3CredentialsFilename != null) {
            creds = new ProfileCredentialsProvider(this.s3CredentialsFilename, "default").getCredentials();
            LOG.info("Writing to S3 using supplied credentials file");
        }
        else {
            // default credentials providers, e.g. IAM role
            creds = new DefaultAWSCredentialsProviderChain().getCredentials();
        }

        ExternalFeedSourceProperty agencyIdProp =
                ExternalFeedSourceProperty.find(feedVersion.getFeedSource(), this.getResourceType(), "AgencyId");

        if(agencyIdProp == null) {
            LOG.error("Could not read AgencyId for FeedSource " + feedVersion.feedSourceId);
            return;
        }

        String keyName = this.s3Prefix + agencyIdProp.value + ".zip";
        LOG.info("Pushing to MTC S3 Bucket: " + keyName);

        AmazonS3 s3client = new AmazonS3Client(creds);
        s3client.putObject(new PutObjectRequest(
                s3Bucket, keyName, feedVersion.getFeed()));

    }
}
