package com.conveyal.datatools.manager.models;

import com.amazonaws.services.ec2.model.Filter;
import com.conveyal.datatools.common.utils.AWSUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.controllers.api.DeploymentController;
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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.mongodb.client.FindIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static final String DEFAULT_OTP_VERSION = "otp-v1.4.0";
    public static final String DEFAULT_R5_VERSION = "v2.4.1-9-g3be6daa";

    /** What server is this currently deployed to? */
    public String deployedTo;

    public List<DeployJob.DeploySummary> deployJobSummaries = new ArrayList<>();

    public String projectId;

    /**
     * Get parent project for deployment. Note: at one point this was a JSON property of this class, but severe
     * performance issues prevent this field from scaling to be fetched/assigned to a large collection of deployments.
     */
    public Project parentProject() {
        return Persistence.projects.getById(projectId);
    }

    @JsonView(JsonViews.DataDump.class)
    public Collection<String> feedVersionIds;

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

    /** All of the feed versions used in this deployment, summarized so that the Internet won't break */
    @JsonProperty("feedVersions")
    public List<SummarizedFeedVersion> retrieveFeedVersions() {
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
    public List<EC2InstanceSummary> retrieveEC2Instances() {
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
        return DeploymentController.fetchEC2InstanceSummaries(
            AWSUtils.getEC2ClientForRole(role, region),
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
     * The version (according to git describe) of OTP being used on this deployment This should default to
     * {@link Deployment#DEFAULT_OTP_VERSION}.
     */
    public String otpVersion;

    public boolean buildGraphOnly;

    /**
     * The version (according to git describe) of R5 being used on this deployment. This should default to
     * {@link Deployment#DEFAULT_R5_VERSION}.
     */
    public String r5Version;

    /** Whether this deployment should build an r5 server (false=OTP) */
    public boolean r5;

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
     * The routerId of this deployment
     */
    public String routerId;

    public String customBuildConfig;
    public String customRouterConfig;

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

    /** Create a single-agency (testing) deployment for the given feed source */
    public Deployment(FeedSource feedSource) {
        super();

        this.feedSourceId = feedSource.id;
        this.projectId = feedSource.projectId;
        this.feedVersionIds = new ArrayList<>();

        DateFormat df = new SimpleDateFormat("yyyyMMdd");

        this.name = StringUtils.getCleanName(feedSource.name) + "_" + df.format(dateCreated);

        // always use the latest, no matter how broken it is, so we can at least see how broken it is
        this.feedVersionIds.add(feedSource.latestVersionId());

        this.routerId = StringUtils.getCleanName(feedSource.name) + "_" + feedSourceId;

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
     * Create an empty deployment, for use with dump/restore.
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
            InputStream is = downloadOsmExtract(retrieveProjectBounds());
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

    /** Convenience method to write serializable object (primarily for router/build config objects) to byte array. */
    private <O extends Serializable> byte[] writeToBytes(O object) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(Include.NON_NULL);
        try {
            return mapper.writer().writeValueAsBytes(object);
        } catch (JsonProcessingException e) {
            LOG.error("Value contains malformed JSON", e);
            return null;
        }
    }

    /** Generate router config for deployment as byte array (for writing to file output stream). */
    public byte[] generateRouterConfig() {
        Project project = this.parentProject();
        return customRouterConfig != null
            ? customRouterConfig.getBytes(StandardCharsets.UTF_8)
            : project.routerConfig != null
                ? writeToBytes(project.routerConfig)
                : null;
    }

    /**
     * Get OSM extract from OSM vex server as input stream.
     */
    public static InputStream downloadOsmExtract(Rectangle2D rectangle2D) throws IOException {
        Bounds bounds = new Bounds(rectangle2D);
        if (!bounds.areValid()) {
            throw new IllegalArgumentException(String.format("Provided bounds %s are not valid", bounds.toVexString()));
        }
        URL vexUrl = new URL(String.format(Locale.ROOT, "%s/%s.pbf",
                DataManager.getConfigPropertyAsText("OSM_VEX"),
                bounds.toVexString()));
        LOG.info("Getting OSM extract at {}", vexUrl.toString());
        HttpURLConnection conn = (HttpURLConnection) vexUrl.openConnection();
        conn.connect();
        return conn.getInputStream();
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
