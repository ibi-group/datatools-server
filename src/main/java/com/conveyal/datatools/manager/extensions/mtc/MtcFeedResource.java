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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;

import static com.conveyal.datatools.manager.models.ExternalFeedSourceProperty.constructId;

/**
 * This class implements the {@link ExternalFeedResource} interface for the MTC RTD database list of carriers (transit
 * operators) and allows the Data Tools application to read and sync the list of carriers to a set of feed sources for a
 * given project.
 *
 * This is generally intended as an initialization step to importing feed sources into a project; however, it should
 * support subsequent sync requests (e.g., if new agencies are expected in the external feed resource, syncing should
 * import those OR if feed properties are expected to have changed in the external feed resource, they should be updated
 * accordingly in Data Tools).
 *
 * Created by demory on 3/30/16.
 */
public class MtcFeedResource implements ExternalFeedResource {

    public static final Logger LOG = LoggerFactory.getLogger(MtcFeedResource.class);

    private String rtdApi, s3Bucket, s3Prefix;

    public static final String AGENCY_ID_FIELDNAME = "AgencyId";
    public static final String RESOURCE_TYPE = "MTC";
    public MtcFeedResource() {
        rtdApi = DataManager.getExtensionPropertyAsText(RESOURCE_TYPE, "rtd_api");
        s3Bucket = DataManager.getExtensionPropertyAsText(RESOURCE_TYPE, "s3_bucket");
        s3Prefix = DataManager.getExtensionPropertyAsText(RESOURCE_TYPE, "s3_prefix");
    }

    @Override
    public String getResourceType() {
        return RESOURCE_TYPE;
    }

    /**
     * Fetch the list of feeds from the MTC endpoint, create any feed sources that do not match on agencyID, and update
     * the external feed source properties.
     */
    @Override
    public void importFeedsForProject(Project project, String authHeader) throws IOException, IllegalAccessException {
        URL url;
        ObjectMapper mapper = new ObjectMapper();
        // A single list of feeds is returned from the MTC Carrier endpoint.
        try {
            url = new URL(rtdApi + "/Carrier");
        } catch(MalformedURLException ex) {
            LOG.error("Could not construct URL for RTD API: {}", rtdApi);
            throw ex;
        }

        try {
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            //add request header
            conn.setRequestProperty("User-Agent", "User-Agent");
            // add auth header
            conn.setRequestProperty("Authorization", authHeader);

            LOG.info("Sending 'GET' request to URL : {}", url);
            LOG.info("Response Code : {}", conn.getResponseCode());

            RtdCarrier[] carriers = mapper.readValue(conn.getInputStream(), RtdCarrier[].class);
            Collection<FeedSource> projectFeedSources = project.retrieveProjectFeedSources();
            // Iterate over carriers found in response and update properties. Also, create a feed source for any carriers
            // found in the response that do not correspond to an agency ID found in the external feed source properties.
            for (int i = 0; i < carriers.length; i++) {
                RtdCarrier carrier = carriers[i];
                FeedSource source = null;

                // Check if a FeedSource with this AgencyId already exists.
                for (FeedSource existingSource : projectFeedSources) {
                    ExternalFeedSourceProperty agencyIdProp;
                    String propertyId = constructId(existingSource, this.getResourceType(), AGENCY_ID_FIELDNAME);
                    agencyIdProp = Persistence.externalFeedSourceProperties.getById(propertyId);
                    if (agencyIdProp != null && agencyIdProp.value != null && agencyIdProp.value.equals(carrier.AgencyId)) {
                        source = existingSource;
                    }
                }
                // Feed source does not exist. Create one using carrier properties.
                if (source == null) {
                    // Derive the name from carrier properties found in response.
                    String feedName = carrier.AgencyName != null
                        ? carrier.AgencyName
                        : carrier.AgencyShortName != null
                            ? carrier.AgencyShortName
                            : carrier.AgencyId;
                    // Create new feed source to store in application database.
                    source = new FeedSource(feedName);
                    source.projectId = project.id;
                    LOG.info("Creating feed source {} from carrier response. (Did not previously exist.)", feedName);
                    // Store the feed source if it does not already exist.
                    Persistence.feedSources.create(source);
                }
                // TODO: Does any property on the feed source need to be updated from the carrier (e.g., name).

                // Create / update the properties
                LOG.info("Updating props for {}", source.name);
                carrier.updateFields(source);
            }
        } catch(Exception ex) {
            LOG.error("Could not read feeds from MTC RTD API");
            throw ex;
        }
    }

    /**
     * Generate blank external feed resource properties when a new feed source is created. Creating a new agency for RTD
     * requires adding the AgencyId property (when it was previously null. See {@link #propertyUpdated(ExternalFeedSourceProperty, String, String)}.
     */
    @Override
    public void feedSourceCreated(FeedSource source, String authHeader) throws IllegalAccessException {
        LOG.info("Processing new FeedSource {} for RTD. Empty external feed properties being generated.", source.name);
        // Create a blank carrier and update fields (will initialize all fields to null).
        RtdCarrier carrier = new RtdCarrier();
        carrier.updateFields(source);
    }

    /**
     * Sync an updated property with the RTD database. Note: if the property is AgencyId and the value was previously
     * null create/register a new carrier with RTD.
     */
    @Override
    public void propertyUpdated(
        ExternalFeedSourceProperty updatedProperty,
        String previousValue,
        String authHeader
    ) throws IOException {
        LOG.info("Update property in MTC carrier table: " + updatedProperty.name);
        String feedSourceId = updatedProperty.feedSourceId;
        FeedSource source = Persistence.feedSources.getById(feedSourceId);
        RtdCarrier carrier = new RtdCarrier(source);

        if(updatedProperty.name.equals(AGENCY_ID_FIELDNAME) && previousValue == null) {
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
                constructId(feedVersion.parentFeedSource(), this.getResourceType(), AGENCY_ID_FIELDNAME)
        );

        if(agencyIdProp == null || agencyIdProp.value.equals("null")) {
            LOG.error("Could not read {} for FeedSource {}", AGENCY_ID_FIELDNAME, feedVersion.feedSourceId);
            return;
        }

        String keyName = String.format("%s%s.zip", this.s3Prefix, agencyIdProp.value);
        LOG.info("Pushing to MTC S3 Bucket: s3://{}/{}", s3Bucket, keyName);
        File file = feedVersion.retrieveGtfsFile();
        try {
            FeedStore.s3Client.putObject(new PutObjectRequest(s3Bucket, keyName, file));
        } catch (Exception e) {
            LOG.error("Could not upload feed version to s3.");
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Update or create a carrier and its properties with an HTTP request to the RTD.
     */
    private void writeCarrierToRtd(RtdCarrier carrier, boolean createNew, String authHeader) throws IOException {

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
            throw e;
        }
    }
}
