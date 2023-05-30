package com.conveyal.datatools.manager.models;

import com.amazonaws.services.ec2.model.Filter;
import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.common.utils.aws.EC2Utils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.jobs.DeployJob;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.StringUtils;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.mongodb.client.FindIterable;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

/**
 * A deployment of (a given version of) OTP on a given set of feeds.
 * @author mattwigway
 *
 */
@JsonInclude(Include.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Deployment extends Model implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Deployment.class);

    public String name;

    //OTP v1.4 is a historical version that was previously used as a fallback. Only use if application default not configured.
    public static final String DEFAULT_OTP_VERSION = getConfigPropertyAsText("application.default_otp_version", "otp-v1.4.0");

    /** What server is this currently deployed to? */
    public String deployedTo;

    public List<DeployJob.DeploySummary> deployJobSummaries = new ArrayList<>();

    public String projectId;

    private ObjectMapper otpConfigMapper = new ObjectMapper().setSerializationInclusion(Include.NON_NULL);

    /* Pelias fields, used to determine where/if to send data to the Pelias webhook */
    public boolean peliasResetDb;
    public List<String> peliasCsvFiles = new ArrayList<>();

    /**
     * Get parent project for deployment. Note: at one point this was a JSON property of this class, but severe
     * performance issues prevent this field from scaling to be fetched/assigned to a large collection of deployments.
     */
    public Project parentProject() {
        return Persistence.projects.getById(projectId);
    }

    @JsonView(JsonViews.DataDump.class)
    public Collection<String> feedVersionIds = new ArrayList<>();

    /** Feed versions that are production ready and should not be replaced by newer versions. */
    public List<String> pinnedfeedVersionIds = new ArrayList<>();

    /** All of the feed versions used in this deployment */
    public List<FeedVersion> retrieveFullFeedVersions() {
        ArrayList<FeedVersion> ret = new ArrayList<>(feedVersionIds.size());

        for (String id : feedVersionIds) {
            FeedVersion v = Persistence.feedVersions.getById(id);
            if (v != null)
                ret.add(v);
            else
                LOG.error("Reference integrity error for deployment {} ({}), feed version {} does not exist", this.name, this.id, id);
        }

        return ret;
    }

    /** Retrieve all of the pinned feed versions used in this deployment. */
    public List<SummarizedFeedVersion> retrievePinnedFeedVersions() {
        return retrieveSummarizedFeedVersions(pinnedfeedVersionIds);
    }

    /** All of the feed versions used in this deployment, summarized so that the Internet won't break */
    @JsonProperty("feedVersions")
    public List<SummarizedFeedVersion> retrieveFeedVersions() {
        return retrieveSummarizedFeedVersions(feedVersionIds);
    }

    /** Retrieve all of the summarized feed versions used in this deployment. */
    private List<SummarizedFeedVersion> retrieveSummarizedFeedVersions(Collection<String> feedVersionIds) {
        // return empty array if feedVersionIds is null
        if (feedVersionIds == null) return new ArrayList<>();

        ArrayList<SummarizedFeedVersion> ret = new ArrayList<>(feedVersionIds.size());

        for (String id : feedVersionIds) {
            FeedVersion v = Persistence.feedVersions.getById(id);

            // should never happen but can if someone monkeyed around with dump/restore
            if (v != null)
                ret.add(new SummarizedFeedVersion(Persistence.feedVersions.getById(id)));
            else
                LOG.error("Reference integrity error for deployment {} ({}), feed version {} does not exist", this.name, this.id, id);
        }

        return ret;
    }

    /** Fetch ec2 instances tagged with this deployment's ID. */
    public List<EC2InstanceSummary> retrieveEC2Instances() throws CheckedAWSException {
        if (!"true".equals(DataManager.getConfigPropertyAsText("modules.deployment.ec2.enabled"))) return Collections.EMPTY_LIST;
        Filter deploymentFilter = new Filter("tag:deploymentId", Collections.singletonList(id));
        // Check if the latest deployment used alternative credentials/AWS role.
        String role = null;
        String region = null;
        if (this.latest() != null) {
            OtpServer server = Persistence.servers.getById(this.latest().serverId);
            if (server != null) {
                role = server.role;
                if (server.ec2Info != null) {
                    region = server.ec2Info.region;
                }
            }
        }
        return EC2Utils.fetchEC2InstanceSummaries(
            EC2Utils.getEC2Client(role, region),
            deploymentFilter
        );
    }

    /**
     * Public URL at which the OSM extract should be downloaded. This should be null if the extract should be downloaded
     * from an extract server. Extract type should be a .pbf.
     */
    public String osmExtractUrl;

    /** If true, OSM extract will be skipped entirely (extract will be fetched from neither extract server nor URL. */
    public boolean skipOsmExtract;

    /**
     * The trip planner to use in an EC2 deployment. otp-runner will be prepared to run a jar file with the proper
     * commands.
     * NOTE: the {@link Deployment#otpVersion} is assumed to be properly set to an appropriate jar file, so it is up to
     * the user to make sure that both the correct jar file and trip planner type combinations are selected.
     */
    public TripPlannerVersion tripPlannerVersion = TripPlannerVersion.OTP_1;

    /**
     * The version (according to git describe) of OTP being used on this deployment. This should default to
     * {@link Deployment#DEFAULT_OTP_VERSION}. This is used to determine what jar file to download and does not have an
     * exact match to actual numbered/tagged releases.
     */
    public String otpVersion = DEFAULT_OTP_VERSION;

    public boolean buildGraphOnly;

    /** Date when the deployment was last deployed to a server */
    @JsonProperty("lastDeployed")
    public Date retrieveLastDeployed () {
        return latest() != null ? new Date(latest().finishTime) : null;
    }

    /** Get latest deployment summary. */
    @JsonProperty("latest")
    public DeployJob.DeploySummary latest () {
        return deployJobSummaries.size() > 0 ? deployJobSummaries.get(0) : null;
    }

    /**
     * The routerId of this deployment. If null, the deployment will use the 'default' router.
     */
    public String routerId;

    public String customBuildConfig;
    public String customRouterConfig;

    public List<CustomFile> customFiles = new ArrayList<>();

    /**
     * If this deployment is for a single feed source, the feed source this deployment is for.
     */
    public String feedSourceId;

    /**
     * Feed sources that had no valid feed versions when this deployment was created, and ergo were not added.
     */
    @JsonInclude(Include.ALWAYS)
    @JsonView(JsonViews.DataDump.class)
    public Collection<String> invalidFeedSourceIds;

    /**
     * Get all of the feed sources which could not be added to this deployment.
     */
    @JsonView(JsonViews.UserInterface.class)
    @JsonInclude(Include.ALWAYS)
    @JsonProperty("invalidFeedSources")
    public List<FeedSource> invalidFeedSources () {
        if (invalidFeedSourceIds == null)
            return null;

        ArrayList<FeedSource> ret = new ArrayList<FeedSource>(invalidFeedSourceIds.size());

        for (String id : invalidFeedSourceIds) {
            ret.add(Persistence.feedSources.getById(id));
        }

        return ret;
    }

    /**
     * Create a single-agency (testing) deployment for the given feed source
     * @param useDefaultRouter should the deployment use the default router (non feed source specific)?
     */
    public Deployment(FeedSource feedSource, boolean useDefaultRouter) {
        super();

        this.feedSourceId = feedSource.id;
        this.projectId = feedSource.projectId;
        this.feedVersionIds = new ArrayList<>();

        DateFormat df = new SimpleDateFormat("yyyyMMdd");

        this.name = StringUtils.getCleanName(feedSource.name) + "_" + df.format(dateCreated);

        // always use the latest, no matter how broken it is, so we can at least see how broken it is
        this.feedVersionIds.add(feedSource.latestVersionId());

        if (!useDefaultRouter) {
            this.routerId = StringUtils.getCleanName(feedSource.name) + "_" + feedSourceId;
        }
        this.deployedTo = null;
    }

    /** Create a new deployment plan for the given feed collection */
    public Deployment(Project project) {
        super();

        this.feedSourceId = null;

        this.projectId = project.id;

        this.feedVersionIds = new ArrayList<>();
        this.invalidFeedSourceIds = new ArrayList<>();

        FEEDSOURCE: for (FeedSource s : project.retrieveProjectFeedSources()) {
            // only include deployable feeds
            if (s.deployable) {
                FeedVersion latest = s.retrieveLatest();

                // find the newest version that can be deployed
                while (true) {
                    if (latest == null) {
                        invalidFeedSourceIds.add(s.id);
                        continue FEEDSOURCE;
                    }

                    if (!latest.hasCriticalErrors()) {
                        break;
                    }

                    latest = latest.previousVersion();
                }

                // this version is the latest good version
                this.feedVersionIds.add(latest.id);
            }
        }

        this.deployedTo = null;
    }

    /**
     * Create an empty deployment, for use with dump/restore and testing.
     */
    public Deployment() {
        // do nothing.
    }

    /**
     * Dump this deployment to the given output file.
     * @param output the output file
     * @param includeOsm should an osm.pbf file be included in the dump?
     * @param includeOtpConfig should OTP build-config.json and router-config.json be included?
     */
    public void dump (File output, boolean includeManifest, boolean includeOsm, boolean includeOtpConfig) throws IOException {
        // Create the zipfile.
        ZipOutputStream out;
        try {
            out = new ZipOutputStream(new FileOutputStream(output));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        if (includeManifest) {
            // save the manifest at the beginning of the file, for read/seek efficiency
            ZipEntry manifestEntry = new ZipEntry("manifest.json");
            out.putNextEntry(manifestEntry);
            // create the json manifest
            JsonManager<Deployment> jsonManifest = new JsonManager<>(Deployment.class, JsonViews.UserInterface.class);
            // this mixin gives us full feed validation results, not summarized
            jsonManifest.addMixin(Deployment.class, DeploymentFullFeedVersionMixin.class);
            byte[] manifest = jsonManifest.write(this).getBytes();
            // Write manifest and close entry.
            out.write(manifest);
            out.closeEntry();
        }

        // Write each of the feed version GTFS files into the zip.
        for (FeedVersion v : this.retrieveFullFeedVersions()) {
            File gtfsFile = v.retrieveGtfsFile();
            FileInputStream in;
            try {
                in = new FileInputStream(gtfsFile);
            } catch (FileNotFoundException e1) {
                LOG.error("Could not retrieve file for {}", v.name);
                throw new RuntimeException(e1);
            }
            ZipEntry e = new ZipEntry(gtfsFile.getName());
            out.putNextEntry(e);
            ByteStreams.copy(in, out);
            try {
                in.close();
            } catch (IOException e1) {
                LOG.warn("Could not close GTFS file input stream {}", gtfsFile.getName());
                e1.printStackTrace();
            }
            out.closeEntry();
        }

        if (includeOsm) {
            // Extract OSM and insert it into the deployment bundle
            ZipEntry e = new ZipEntry("osm.pbf");
            out.putNextEntry(e);
            InputStream is = downloadOsmExtract();
            ByteStreams.copy(is, out);
            try {
                is.close();
            } catch (IOException e1) {
                LOG.warn("Could not close OSM input stream");
                e1.printStackTrace();
            }
            out.closeEntry();
        }

        if (includeOtpConfig) {
            // Write build-config.json and router-config.json into zip file.
            // Use custom build config if it is not null, otherwise default to project build config.
            byte[] buildConfigAsBytes = generateBuildConfig();
            if (buildConfigAsBytes != null) {
                // Include build config if not null.
                ZipEntry buildConfigEntry = new ZipEntry("build-config.json");
                out.putNextEntry(buildConfigEntry);
                out.write(buildConfigAsBytes);
                out.closeEntry();
            }
            // Use custom router config if it is not null, otherwise default to project router config.
            byte[] routerConfigAsBytes = generateRouterConfig();
            if (routerConfigAsBytes != null) {
                // Include router config if not null.
                ZipEntry routerConfigEntry = new ZipEntry("router-config.json");
                out.putNextEntry(routerConfigEntry);
                out.write(routerConfigAsBytes);
                out.closeEntry();
            }
        }
        // Finally close the zip output stream. The dump file is now complete.
        out.close();
    }

    /** Generate build config for deployment as byte array (for writing to file output stream). */
    public byte[] generateBuildConfig() {
        Project project = this.parentProject();
        return customBuildConfig != null
            ? customBuildConfig.getBytes(StandardCharsets.UTF_8)
            : project.buildConfig != null
                ? writeToBytes(project.buildConfig)
                : null;
    }

    public String generateBuildConfigAsString() {
        if (customBuildConfig != null) return customBuildConfig;
        return writeToString(this.parentProject().buildConfig);
    }

    /** Convenience method to write serializable object (primarily for router/build config objects) to byte array. */
    private <O extends Serializable> byte[] writeToBytes(O object) {
        try {
            return otpConfigMapper.writer().writeValueAsBytes(object);
        } catch (JsonProcessingException e) {
            LOG.error("Value contains malformed JSON", e);
            return null;
        }
    }

    /** Convenience method to write serializable object (primarily for router/build config objects) to string. */
    private <O extends Serializable> String writeToString(O object) {
        try {
            return otpConfigMapper.writer().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            LOG.error("Value contains malformed JSON", e);
            return null;
        }
    }

    /** Generate router config for deployment as string. */
    public byte[] generateRouterConfig() throws IOException {
        Project project = this.parentProject();

        byte[] customRouterConfigString = customRouterConfig != null
                ? customRouterConfig.getBytes(StandardCharsets.UTF_8)
                : null;

        byte[] routerConfigString = project.routerConfig != null
                ? writeToBytes(project.routerConfig)
                : null;

        // If both router configs are present, merge the JSON before returning
        // Merger code from: https://stackoverflow.com/questions/35747813/how-to-merge-two-json-strings-into-one-in-java
        if (customRouterConfigString != null && routerConfigString != null) {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map1 = mapper.readValue(customRouterConfigString, Map.class);
            Map<String, Object> map2 = mapper.readValue(routerConfigString, Map.class);
            Map<String, Object> merged = new HashMap<String, Object>(map2);
            merged.putAll(map1);
            return mapper.writeValueAsString(merged).getBytes();
        }

        return customRouterConfigString != null
            ? customRouterConfigString
            : routerConfigString != null
                ? routerConfigString
                : null;
    }

    /** Generate router config for deployment as byte array (for writing to file output stream). */
    public String generateRouterConfigAsString() {
            try {
                return new String(generateRouterConfig(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                LOG.error("Failed to generate router config: ", e);
                return "";
        }
    }

    /**
     * Get OSM extract from OSM extract URL (vex server or static URL) as input stream.
     */
    public InputStream downloadOsmExtract() throws IOException {
        URL extractUrl = getUrlForOsmExtract();
        if (extractUrl == null) {
            throw new IllegalArgumentException("Cannot download OSM extract. Extract URL is invalid.");
        }
        LOG.info("Getting OSM extract at {}", extractUrl.toString());
        HttpURLConnection conn = (HttpURLConnection) extractUrl.openConnection();
        conn.connect();
        return conn.getInputStream();
    }

    /**
     * Gets the URL for downloading an OSM PBF file from the osm vex server for the desired bounding box.
     */
    public static URL getVexUrl (Rectangle2D rectangle2D) throws MalformedURLException {
        Bounds bounds = new Bounds(rectangle2D);
        if (!bounds.areValid()) {
            throw new IllegalArgumentException(String.format("Provided bounds %s are not valid", bounds.toVexString()));
        }
        return new URL(String.format(Locale.ROOT, "%s/%s.pbf",
            DataManager.getConfigPropertyAsText("OSM_VEX"),
            bounds.toVexString()));
    }

    /**
     * Gets the preferred extract URL for a deployment. If {@link #skipOsmExtract} is true or the osmExtractUrl or vex URL
     * is invalid, this will return null.
     *
     * Note: this method name must not be getOsmExtractUrl because {@link #osmExtractUrl} is an instance field and ignore
     * annotations will cause it to disappear during de-/serialization.
     */
    @JsonIgnore
    @BsonIgnore
    public URL getUrlForOsmExtract() throws MalformedURLException {
        // Return null if deployment should skip extract.
        if (skipOsmExtract) return null;
        URL osmUrl = null;
        // Otherwise, prefer the static extract URL if defined.
        if (osmExtractUrl != null) {
            try {
                osmUrl = new URL(osmExtractUrl);
            } catch (MalformedURLException e) {
                LOG.error("Could not construct extract URL from {}", osmExtractUrl, e);
            }
        } else {
            // Finally, if no custom extract URL is provided, default to a vex URL.
            osmUrl = getVexUrl(retrieveProjectBounds());
        }
        return osmUrl;
    }

    /**
     * Get the union of the bounds of all the feed versions in this deployment or if using custom bounds, return the
     * project's custom bounds.
     */
    @JsonView(JsonViews.UserInterface.class)
    @JsonProperty("projectBounds")
    public Rectangle2D retrieveProjectBounds() {

        Project project = this.parentProject();
        if(project.useCustomOsmBounds && project.bounds != null) {
            // Simply return the project's custom bounds if not determining bounds via feed version bounds.
            return project.bounds.toRectangle2D();
        }

        List<SummarizedFeedVersion> versions = retrieveFeedVersions();

        if (versions.size() == 0)
            return null;

        Rectangle2D bounds = new Rectangle2D.Double();
        boolean boundsSet = false;

        // i = 1 because we've already included bounds 0
        for (int i = 0; i < versions.size(); i++) {
            SummarizedFeedVersion version = versions.get(i);

            // set version bounds from validation result
            if (version.boundsAreValid()) {
                if (!boundsSet) {
                    // set the bounds, don't expand the null bounds
                    bounds.setRect(versions.get(0).validationResult.bounds.toRectangle2D());
                    boundsSet = true;
                } else {
                    bounds.add(version.validationResult.bounds.toRectangle2D());
                }
            } else {
                LOG.warn("Feed version {} has no bounds", version.id);
            }
        }

        // expand the bounds by (about) 10 km in every direction
        double degreesPerKmLat = 360D / 40008;
        double degreesPerKmLon =
                // the circumference of the chord of the earth at this latitude
                360 /
                (2 * Math.PI * 6371 * Math.cos(Math.toRadians(bounds.getCenterY())));


        double bufferKm = 10;
        if(DataManager.hasConfigProperty("modules.deployment.osm_buffer_km")) {
            bufferKm = DataManager.getConfigProperty("modules.deployment.osm_buffer_km").asDouble();
        }

        // south-west
        bounds.add(new Point2D.Double(
                // lon
                bounds.getMinX() - bufferKm * degreesPerKmLon,
                bounds.getMinY() - bufferKm * degreesPerKmLat
                ));

        // north-east
        bounds.add(new Point2D.Double(
                // lon
                bounds.getMaxX() + bufferKm * degreesPerKmLon,
                bounds.getMaxY() + bufferKm * degreesPerKmLat
                ));

        return bounds;
    }

    /**
     * Get the deployments currently deployed to a particular server and router combination.
     */
    public static FindIterable<Deployment> retrieveDeploymentForServerAndRouterId(String serverId, String routerId) {
        return Persistence.deployments.getMongoCollection().find(and(
                eq("deployedTo", serverId),
                eq("routerId", routerId)
        ));
    }

    @JsonProperty("organizationId")
    public String organizationId() {
        Project project = parentProject();
        return project == null ? null : project.organizationId;
    }

    public boolean delete() {
        return Persistence.deployments.removeById(this.id);
    }

    /**
     * A summary of a FeedVersion, leaving out all of the individual validation errors.
     */
    public static class SummarizedFeedVersion {
        public FeedValidationResultSummary validationResult;
        public FeedSource feedSource;
        public String id;
        public Date updated;
        public String previousVersionId;
        public String nextVersionId;
        public int version;

        /** No-arg constructor for de-/serialization. */
        public SummarizedFeedVersion() { }

        public SummarizedFeedVersion (FeedVersion version) {
            this.validationResult = new FeedValidationResultSummary(version);
            this.feedSource = version.parentFeedSource();
            this.updated = version.updated;
            this.id = version.id;
            this.nextVersionId = version.nextVersionId();
            this.previousVersionId = version.previousVersionId();
            this.version = version.version;
        }

        /**
         * Determine if the bounds for the summary version exist and are valid.
         */
        public boolean boundsAreValid () {
            return validationResult != null && validationResult.bounds != null && validationResult.bounds.areValid();
        }
    }

    public enum TripPlannerVersion {
        OTP_1, OTP_2
    }

    /**
     * A MixIn to be applied to this deployment, for generating manifests, so that full feed versions appear rather than
     * summarized feed versions.
     *
     * Usually a mixin would be used on an external class, but since we are changing one thing about a single class, it seemed
     * unnecessary to define a new view just for generating deployment manifests.
     */
    public abstract static class DeploymentFullFeedVersionMixin {
        @JsonIgnore
        public abstract Collection<SummarizedFeedVersion> retrieveFeedVersions();

        @JsonProperty("feedVersions")
        @JsonIgnore(false)
        public abstract Collection<FeedVersion> retrieveFullFeedVersions ();
    }

    /**
     * A MixIn to be applied to this deployment, for returning a single deployment, so that the list of ec2Instances is
     * included in the JSON response.
     *
     * Usually a mixin would be used on an external class, but since we are changing one thing about a single class, it seemed
     * unnecessary to define a new view.
     */
    public abstract static class DeploymentWithEc2InstancesMixin {

        @JsonProperty("ec2Instances")
        public abstract Collection<FeedVersion> retrieveEC2Instances ();
    }
}
