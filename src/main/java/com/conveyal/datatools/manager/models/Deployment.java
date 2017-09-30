package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.persistence.DataStore;
import com.conveyal.datatools.manager.utils.StringUtils;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
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
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static DataStore<Deployment> deploymentStore = new DataStore<Deployment>("deployments");

    public String name;

    /** What server is this currently deployed to? */
    public String deployedTo;

    public Date dateCreated;

    @JsonView(JsonViews.DataDump.class)
    public String projectId;

    @JsonView(JsonViews.UserInterface.class)
    public Project getProject () {
        return Project.get(projectId);
    }

    public void setProject (Project project) {
        this.projectId = project.id;
    }

    @JsonView(JsonViews.DataDump.class)
    public Collection<String> feedVersionIds;

    /** All of the feed versions used in this deployment */
    @JsonIgnore
    public List<FeedVersion> getFullFeedVersions () {
        ArrayList<FeedVersion> ret = new ArrayList<FeedVersion>(feedVersionIds.size());

        for (String id : feedVersionIds) {
            FeedVersion v = FeedVersion.get(id);
            if (v != null)
                ret.add(v);
            else
                LOG.error("Reference integrity error for deployment {} ({}), feed version {} does not exist", this.name, this.id, id);
        }

        return ret;
    }

    /** All of the feed versions used in this deployment, summarized so that the Internet won't break */
    @JsonView(JsonViews.UserInterface.class)
    public List<SummarizedFeedVersion> getFeedVersions () {
        ArrayList<SummarizedFeedVersion> ret = new ArrayList<SummarizedFeedVersion>(feedVersionIds.size());

        for (String id : feedVersionIds) {
            FeedVersion v = FeedVersion.get(id);

            // should never happen but can if someone monkeyed around with dump/restore
            if (v != null)
                ret.add(new SummarizedFeedVersion(FeedVersion.get(id)));
            else
                LOG.error("Reference integrity error for deployment {} ({}), feed version {} does not exist", this.name, this.id, id);
        }

        return ret;
    }

    public void setFeedVersions (Collection<FeedVersion> versions) {
        feedVersionIds = new ArrayList<String>(versions.size());

        for (FeedVersion version : versions) {
            feedVersionIds.add(version.id);
        }
    }

    // future use
    public String osmFileId;

    /** The commit of OTP being used on this deployment */
    public String otpCommit;

    /**
     * The routerId of this deployment
     */
    public String routerId;

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
    public List<FeedSource> getInvalidFeedSources () {
        if (invalidFeedSourceIds == null)
            return null;

        ArrayList<FeedSource> ret = new ArrayList<FeedSource>(invalidFeedSourceIds.size());

        for (String id : invalidFeedSourceIds) {
            ret.add(FeedSource.get(id));
        }

        return ret;
    }

    /** Create a single-agency (testing) deployment for the given feed source */
    public Deployment(FeedSource feedSource) {
        super();

        this.feedSourceId = feedSource.id;
        this.setProject(feedSource.getProject());
        this.dateCreated = new Date();
        this.feedVersionIds = new ArrayList<String>();

        DateFormat df = new SimpleDateFormat("yyyyMMdd");

        this.name = StringUtils.getCleanName(feedSource.name) + "_" + df.format(dateCreated);

        // always use the latest, no matter how broken it is, so we can at least see how broken it is
        this.feedVersionIds.add(feedSource.getLatestVersionId());

        this.routerId = StringUtils.getCleanName(feedSource.name) + "_" + feedSourceId;

        this.deployedTo = null;
    }

    /** Create a new deployment plan for the given feed collection */
    public Deployment(Project project) {
        super();

        this.feedSourceId = null;

        this.setProject(project);

        this.dateCreated = new Date();

        this.feedVersionIds = new ArrayList<String>();
        this.invalidFeedSourceIds = new ArrayList<String>();

        FEEDSOURCE: for (FeedSource s : project.getProjectFeedSources()) {
            // only include deployable feeds
            if (s.deployable) {
                FeedVersion latest = s.getLatest();

                // find the newest version that can be deployed
                while (true) {
                    if (latest == null) {
                        invalidFeedSourceIds.add(s.id);
                        continue FEEDSOURCE;
                    }

                    if (!latest.hasCriticalErrors()) {
                        break;
                    }

                    latest = latest.getPreviousVersion();
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

    /** Get a deployment by ID */
    public static Deployment get (String id) {
        return deploymentStore.getById(id);
    }

    /** Save this deployment and commit it */
    public void save () {
        this.save(true);
    }

    /** Save this deployment */
    public void save (boolean commit) {
        if (commit)
            deploymentStore.save(id, this);
        else
            deploymentStore.saveWithoutCommit(id, this);
    }

    /**
     * Delete this deployment and everything that it contains.
     */
    public void delete() {
        deploymentStore.delete(this.id);
    }

    /** Dump this deployment to the given file
     * @param output the output file
     * @param includeOsm should an osm.pbf file be included in the dump?
     * @param includeOtpConfig should OTP build-config.json and router-config.json be included?
     */
    public void dump (File output, boolean includeManifest, boolean includeOsm, boolean includeOtpConfig) throws IOException {
        // create the zipfile
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
            JsonManager<Deployment> jsonManifest = new JsonManager<Deployment>(Deployment.class,
                    JsonViews.UserInterface.class);
            // this mixin gives us full feed validation results, not summarized
            jsonManifest.addMixin(Deployment.class, DeploymentFullFeedVersionMixin.class);

            byte[] manifest = jsonManifest.write(this).getBytes();

            out.write(manifest);

            out.closeEntry();
        }

        // write each of the GTFS feeds
        for (FeedVersion v : this.getFullFeedVersions()) {
            File feed = v.getGtfsFile();

            FileInputStream in;

            try {
                in = new FileInputStream(feed);
            } catch (FileNotFoundException e1) {
                LOG.error("Could not retrieve file for {}", v.name);
                throw new RuntimeException(e1);
            }

            ZipEntry e = new ZipEntry(feed.getName());
            out.putNextEntry(e);

            // copy the zipfile 100k at a time
            int bufSize = 100 * 1024;
            byte[] buff = new byte[bufSize];
            int readBytes;

            while (true) {
                try {
                    readBytes = in.read(buff);
                } catch (IOException e1) {
                    try {
                        in.close();
                    } catch (IOException e2) {
                        throw new RuntimeException(e2);
                    }
                    throw new RuntimeException(e1);
                }

                if (readBytes == -1)
                    // we've copied the whole file
                    break;

                out.write(buff, 0, readBytes);
            }

            try {
                in.close();
            } catch (IOException e1) {
                throw new RuntimeException(e1);
            }

            out.closeEntry();
        }

        if (includeOsm) {
            // extract OSM and insert it into the deployment bundle
            ZipEntry e = new ZipEntry("osm.pbf");
            out.putNextEntry(e);
            InputStream is = getOsmExtract(getProjectBounds());
            ByteStreams.copy(is, out);
            try {
                is.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }

            out.closeEntry();
        }

        if (includeOtpConfig) {
            // write build-config.json and router-config.json
            Project proj = this.getProject();

            if (proj.buildConfig != null) {
                ZipEntry buildConfigEntry = new ZipEntry("build-config.json");
                out.putNextEntry(buildConfigEntry);

                ObjectMapper mapper = new ObjectMapper();
                mapper.setSerializationInclusion(Include.NON_NULL);
                byte[] buildConfig = mapper.writer().writeValueAsBytes(proj.buildConfig);
                out.write(buildConfig);

                out.closeEntry();
            }
            // TODO: remove branding url root here and from config.yml
            String brandingUrlRoot = DataManager.getConfigPropertyAsText("application.data.branding_public");
            OtpRouterConfig routerConfig = proj.routerConfig;
            if (routerConfig == null && brandingUrlRoot != null) {
                routerConfig = new OtpRouterConfig();
            }
            if (routerConfig != null) {
                routerConfig.brandingUrlRoot = brandingUrlRoot;
                ZipEntry routerConfigEntry = new ZipEntry("router-config.json");
                out.putNextEntry(routerConfigEntry);

                ObjectMapper mapper = new ObjectMapper();
                mapper.setSerializationInclusion(Include.NON_NULL);
                out.write(mapper.writer().writeValueAsBytes(routerConfig));

                out.closeEntry();
            }
        }

        out.close();
    }

    // Get OSM extract
    public static InputStream getOsmExtract(Rectangle2D bounds) {
        // call vex server
        URL vexUrl = null;
        try {
            vexUrl = new URL(String.format(Locale.ROOT,"%s/%.6f,%.6f,%.6f,%.6f.pbf",
                    DataManager.getConfigPropertyAsText("OSM_VEX"),
                    bounds.getMinY(), bounds.getMinX(), bounds.getMaxY(), bounds.getMaxX()));
        } catch (MalformedURLException e1) {
            e1.printStackTrace();
        }
        LOG.info("Getting OSM extract at " + vexUrl.toString());
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) vexUrl.openConnection();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        try {
            conn.connect();
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        InputStream is = null;
        try {
            is = conn.getInputStream();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        return is;
    }

    public static Rectangle2D getFeedVersionBounds(FeedVersion version) {
        return null;
    }

    // Get the union of the bounds of all the feeds in this deployment
    @JsonView(JsonViews.UserInterface.class)
    public Rectangle2D getProjectBounds() {

        Project proj = this.getProject();
        if(proj.useCustomOsmBounds != null && proj.useCustomOsmBounds) {
            Rectangle2D bounds = new Rectangle2D.Double(proj.osmWest, proj.osmSouth,
                    proj.osmEast - proj.osmWest, proj.osmNorth - proj.osmSouth);
            return bounds;
        }

        List<SummarizedFeedVersion> versions = getFeedVersions();

        if (versions.size() == 0)
            return null;

        Rectangle2D bounds = new Rectangle2D.Double();
        boolean boundsSet = false;

        // i = 1 because we've already included bounds 0
        for (int i = 0; i < versions.size(); i++) {
            SummarizedFeedVersion version = versions.get(i);

            // set version bounds from validation result
            if (version.validationResult != null && version.validationResult.bounds != null) {
                if (!boundsSet) {
                    // set the bounds, don't expand the null bounds
                    bounds.setRect(versions.get(0).validationResult.bounds);
                    boundsSet = true;
                } else {
                    bounds.add(version.validationResult.bounds);
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
     * Commit changes to the datastore
     */
    public static void commit () {
        deploymentStore.commit();
    }

    /**
     * Get all of the deployments.
     */
    public static Collection<Deployment> getAll () {
        return deploymentStore.getAll();
    }

    /**
     * Get the deployment currently deployed to a particular server.
     */
    public static Deployment getDeploymentForServerAndRouterId (String server, String routerId) {
        for (Deployment d : getAll()) {
            if (d.deployedTo != null && d.deployedTo.equals(server)) {
                if ((routerId != null && routerId.equals(d.routerId)) || d.routerId == routerId) {
                    return d;
                }
            }
        }

        return null;
    }

    public String getOrganizationId () {
        Project project = getProject();
        return project == null ? null : project.organizationId;
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
            this.validationResult = new FeedValidationResultSummary(version.validationResult);
            this.feedSource = version.getFeedSource();
            this.updated = version.updated;
            this.id = version.id;
            this.nextVersionId = version.getNextVersionId();
            this.previousVersionId = version.getPreviousVersionId();
            this.version = version.version;
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
        public abstract Collection<SummarizedFeedVersion> getFeedVersions();

        @JsonProperty("feedVersions")
        @JsonIgnore(false)
        public abstract Collection<FeedVersion> getFullFeedVersions ();
    }
}
