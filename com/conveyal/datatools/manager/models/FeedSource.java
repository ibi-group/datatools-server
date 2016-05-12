package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.jobs.BuildTransportNetworkJob;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.persistence.DataStore;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;
import org.mapdb.Atomic;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

import static com.conveyal.datatools.manager.utils.NotificationsUtils.notifyUsersForSubscription;

/**
 * Created by demory on 3/22/16.
 */
public class FeedSource extends Model {
    private static final long serialVersionUID = 1L;

    public static final Logger LOG = LoggerFactory.getLogger(FeedSource.class);

    private static DataStore<FeedSource> sourceStore = new DataStore<FeedSource>("feedsources");

    /**
     * The collection of which this feed is a part
     */
    //@JsonView(JsonViews.DataDump.class)
    public String projectId;

    public String[] regions = {"1"};
    /**
     * Get the Project of which this feed is a part
     */
    @JsonIgnore
    public Project getProject () {
        return Project.get(projectId);
    }

    @JsonIgnore
    public List<Region> getRegionList () {
        return Region.getAll().stream().filter(r -> Arrays.asList(regions).contains(r.id)).collect(Collectors.toList());
    }

    public void setProject(Project proj) {
        this.projectId = proj.id;
        this.save();
        proj.save();
    }

    /** The name of this feed source, e.g. MTA New York City Subway */
    public String name;

    /** Is this feed public, i.e. should it be listed on the
     * public feeds page for download?
     */
    public boolean isPublic;

    /** Is this feed deployable? */
    public boolean deployable;

    /**
     * How do we receive this feed?
     */
    public FeedRetrievalMethod retrievalMethod;

    /**
     * When was this feed last fetched?
     */
    public Date lastFetched;

    /**
     * When was this feed last updated?
     */
    //public transient Date lastUpdated;

    /**
     * From whence is this feed fetched?
     */
    public URL url;

    /**
     * What is the GTFS Editor snapshot for this feed?
     *
     * This is the String-formatted snapshot ID, which is the base64-encoded ID and the version number.
     */
    public String snapshotVersion;

    //public Collection<AgencyBranding> branding;

    /**
     * Create a new feed.
     */
    public FeedSource (String name) {
        super();
        this.name = name;
        this.retrievalMethod = FeedRetrievalMethod.MANUALLY_UPLOADED;
    }

    /**
     * No-arg constructor to yield an uninitialized feed source, for dump/restore.
     * Should not be used in general code.
     */
    public FeedSource () {
        this(null);
    }

    /**
     * Fetch the latest version of the feed.
     */
    public FeedVersion fetch () {
        if (this.retrievalMethod.equals(FeedRetrievalMethod.MANUALLY_UPLOADED)) {
            LOG.info("not fetching feed {}, not a fetchable feed", this.toString());
            return null;
        }

        // fetchable feed, continue
        FeedVersion latest = getLatest();

        // We create a new FeedVersion now, so that the fetched date is (milliseconds) before
        // fetch occurs. That way, in the highly unlikely event that a feed is updated while we're
        // fetching it, we will not miss a new feed.
        FeedVersion newFeed = new FeedVersion(this);

        // build the URL from which to fetch
        URL url;
        if (this.retrievalMethod.equals(FeedRetrievalMethod.FETCHED_AUTOMATICALLY))
            url = this.url;
        else if (this.retrievalMethod.equals(FeedRetrievalMethod.PRODUCED_IN_HOUSE)) {
            if (this.snapshotVersion == null) {
                LOG.error("Feed {} has no editor id; cannot fetch", this);
                return null;
            }

            String baseUrl = DataManager.getConfigPropertyAsText("modules.editor.url");

            if (!baseUrl.endsWith("/"))
                baseUrl += "/";

            // build the URL
            try {
                url = new URL(baseUrl + "api/mgrsnapshot/" + this.snapshotVersion + ".zip");
            } catch (MalformedURLException e) {
                LOG.error("Invalid URL for editor, check your config.");
                return null;
            }
        }
        else {
            LOG.error("Unknown retrieval method" + this.retrievalMethod);
            return null;
        }

        LOG.info(url.toString());

        // make the request, using the proper HTTP caching headers to prevent refetch, if applicable
        HttpURLConnection conn;
        try {
            conn = (HttpURLConnection) url.openConnection();
        } catch (IOException e) {
            LOG.error("Unable to open connection to {}; not fetching feed {}", url, this);
            return null;
        } catch (ClassCastException e) {
            LOG.error("Unable to open connection to {}; not fetching feed {}", url, this);
            return null;
        } catch (NullPointerException e) {
            LOG.error("Unable to open connection to {}; not fetching feed {}", url, this);
            return null;
        }

        conn.setDefaultUseCaches(true);

        /*if (oauthToken != null)
            conn.addRequestProperty("Authorization", "Bearer " + oauthToken);*/

        // lastFetched is set to null when the URL changes
        if (latest != null && this.lastFetched != null)
            conn.setIfModifiedSince(Math.min(latest.updated.getTime(), this.lastFetched.getTime()));

        try {
            conn.connect();

            if (conn.getResponseCode() == HttpURLConnection.HTTP_NOT_MODIFIED) {
                LOG.info("Feed {} has not been modified", this);
                return null;
            }

            // TODO: redirects
            else if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                LOG.info("Saving feed {}", this);

                File out = newFeed.newFeed(conn.getInputStream());

            }

            else {
                LOG.error("HTTP status {} retrieving feed {}", conn.getResponseMessage(), this);
                return null;
            }
        } catch (IOException e) {
            LOG.error("Unable to connect to {}; not fetching feed {}", url, this);
            return null;
        }

        // validate the fetched file
        // note that anything other than a new feed fetched successfully will have already returned from the function
        newFeed.hash();

        if (latest != null && newFeed.hash.equals(latest.hash)) {
            LOG.warn("Feed {} was fetched but has not changed; server operators should add If-Modified-Since support to avoid wasting bandwidth", this);
            newFeed.getFeed().delete();
            return null;
        }
        else {
            newFeed.userId = this.userId;

            new ProcessSingleFeedJob(newFeed).run();
            if (DataManager.config.get("modules").get("validator").get("enabled").asBoolean()) {
//                new BuildTransportNetworkJob(newFeed).run();
                BuildTransportNetworkJob btnj = new BuildTransportNetworkJob(newFeed);
                Thread tnThread = new Thread(btnj);
                tnThread.start();
            }

            this.lastFetched = newFeed.updated;
            this.save();
            notifyUsersForSubscription("feed-updated", this.id);
            return newFeed;
        }
    }

    public int compareTo(FeedSource o) {
        return this.name.compareTo(o.name);
    }

    public String toString () {
        return "<FeedSource " + this.name + " (" + this.id + ")>";
    }

    public void save () {
        save(true);
    }
    public void setName(String name){
        this.name = name;
        this.save();
    }
    public void save (boolean commit) {
        if (commit)
            sourceStore.save(this.id, this);
        else
            sourceStore.saveWithoutCommit(this.id, this);
    }

    /**
     * Get the latest version of this feed
     * @return the latest version of this feed
     */
    @JsonIgnore
    public FeedVersion getLatest () {
//        DataStore<FeedVersion> vs = new FeedVersion(this);
//        if (vs == null){
//            return null;
//        }
        FeedVersion v = FeedVersion.versionStore.findFloor("version", new Fun.Tuple2(this.id, Fun.HI));

        // the ID doesn't necessarily match, because it will fall back to the previous source in the store if there are no versions for this source
        if (v == null || !v.feedSourceId.equals(this.id))
            return null;

        return v;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    public String getLatestVersionId () {
        FeedVersion latest = getLatest();
        return latest != null ? latest.id : null;
    }

    /**
     * We can't pass the entire latest feed version back, because it contains references back to this feedsource,
     * so Jackson doesn't work. So instead we specifically expose the validation results and the latest update.
     * @return
     */
    // TODO: use summarized feed source here. requires serious refactoring on client side.
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    public Date getLastUpdated() {
        FeedVersion latest = getLatest();
        return latest != null ? latest.updated : null;
    }


    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    public FeedValidationResultSummary getLatestValidation () {
        FeedVersion latest = getLatest();
        return latest != null ? new FeedValidationResultSummary(latest.validationResult) : null;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonView(JsonViews.UserInterface.class)
    public Map<String, Map<String, String>> getExternalProperties() {

        Map<String, Map<String, String>> resourceTable = new HashMap<>();

        for(String resourceType : DataManager.feedResources.keySet()) {
            Map<String, String> propTable = new HashMap<>();

            for (ExternalFeedSourceProperty prop : ExternalFeedSourceProperty.getAll()) {
                if (prop.getFeedSourceId().equals(this.id)) {
                    propTable.put(prop.name, prop.value);
                }
            }

            resourceTable.put(resourceType, propTable);
        }
        return resourceTable;
    }

    public static FeedSource get(String id) {
        return sourceStore.getById(id);
    }

    public static Collection<FeedSource> getAll() {
        return sourceStore.getAll();
    }

    /**
     * Get all of the feed versions for this source
     * @return
     */
    @JsonIgnore
    public Collection<FeedVersion> getFeedVersions() {
        // TODO Indices
        ArrayList<FeedVersion> ret = new ArrayList<FeedVersion>();

        for (FeedVersion v : FeedVersion.getAll()) {
            if (this.id.equals(v.feedSourceId)) {
                ret.add(v);
            }
        }

        return ret;
    }

    @JsonView(JsonViews.UserInterface.class)
    public int getNoteCount() {
        return this.noteIds != null ? this.noteIds.size() : 0;
    }

    /**
     * Represents ways feeds can be retrieved
     */
    public static enum FeedRetrievalMethod {
        FETCHED_AUTOMATICALLY, // automatically retrieved over HTTP on some regular basis
        MANUALLY_UPLOADED, // manually uploaded by someone, perhaps the agency, or perhaps an internal user
        PRODUCED_IN_HOUSE // produced in-house in a GTFS Editor instance
    }

    public static void commit() {
        sourceStore.commit();
    }

    /**
     * Delete this feed source and everything that it contains.
     */
    public void delete() {
        for (FeedVersion v : getFeedVersions()) {
            v.delete();
        }

        for (ExternalFeedSourceProperty prop : ExternalFeedSourceProperty.getAll()) {
            if(prop.getFeedSourceId().equals(this.id)) {
                prop.delete();
            }
        }

        sourceStore.delete(this.id);
    }

    /*@JsonIgnore
    public AgencyBranding getAgencyBranding(String agencyId) {
        if(branding != null) {
            for (AgencyBranding agencyBranding : branding) {
                if (agencyBranding.agencyId.equals(agencyId)) return agencyBranding;
            }
        }
        return null;
    }

    @JsonIgnore
    public void addAgencyBranding(AgencyBranding agencyBranding) {
        if(branding == null) {
            branding = new ArrayList<>();
        }
        branding.add(agencyBranding);
    }*/

}
