package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.common.utils.Scheduler;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HashUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.http.Header;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class FetchSingleFeedJob extends MonitorableJob {

    private FeedSource feedSource;
    private FeedVersion result;
    private boolean continueThread;
    private static final Logger LOG = LoggerFactory.getLogger(FetchSingleFeedJob.class);
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    private HttpGet httpGet;

    /**
     * Fetch a single feed source by URL
     * @param feedSource feed source to be fetched
     * @param owner user who owns job
     */
    public FetchSingleFeedJob (FeedSource feedSource, Auth0UserProfile owner, boolean continueThread) {
        super(owner, "Fetching feed for " + feedSource.name, JobType.FETCH_SINGLE_FEED);
        this.feedSource = feedSource;
        this.result = null;
        this.continueThread = continueThread;
        status.message = "Fetching...";
        status.percentComplete = 0.0;
        status.uploading = true;
    }

    /**
     * Getter that allows a client to know the ID of the feed version that will be created as soon as the upload is
     * initiated; however, we will not store the FeedVersion in the mongo application database until the upload and
     * processing is completed. This prevents clients from manipulating GTFS data before it is entirely imported.
     */
    @JsonProperty
    public String getFeedVersionId () {
        // Feed version result is null unless (and until) fetch is successful.
        return result != null ? result.id : null;
    }

    @JsonProperty
    public String getFeedSourceId () {
        // Feed version result is null unless (and until) fetch is successful.
        return result != null ? result.parentFeedSource().id : null;
    }

    public FeedVersion fetch (FeedSource feedSource) {
        return fetch(feedSource, null);
    }
    /**
     * Fetch the latest version of the feed. Optionally provide an override URL from which to fetch the feed. This
     * optional URL is used for a one-level deep recursive call of fetch when a redirect is encountered.
     *
     * FIXME: Should the FeedSource fetch URL field be updated if a recursive call with new URL is successful?
     *
     * @return the fetched FeedVersion if a new version is available or null if nothing needs to be updated.
     */
    public FeedVersion fetch (FeedSource feedSource, String optionalUrlOverride) {
        status.message = "Downloading file";

        // We create a new FeedVersion now, so that the fetched date is (milliseconds) before
        // fetch occurs. That way, in the highly unlikely event that a feed is updated while we're
        // fetching it, we will not miss a new feed.
        FeedVersion version = new FeedVersion(feedSource);
        version.retrievalMethod = FeedSource.FeedRetrievalMethod.FETCHED_AUTOMATICALLY;

        // build the URL from which to fetch
        URL url = null;
        try {
            // If an optional URL is provided (in the case of a recursive fetch) use that. Otherwise, use the fetch URL
            url = optionalUrlOverride != null ? new URL(optionalUrlOverride) : feedSource.url;
        } catch (MalformedURLException e) {
            e.printStackTrace();
            status.fail(String.format("Could not connect to bad redirect URL %s", optionalUrlOverride));
        }
        LOG.info("Fetching from {}", url.toString());

        // make the request, using the proper HTTP caching headers to prevent refetch, if applicable
        httpGet = new HttpGet(url.toString());
        httpGet.addHeader(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11"
        );
        // Get latest version to check that the fetched version does not duplicate a feed already loaded.
        FeedVersion latest = feedSource.retrieveLatest();
        // lastFetched is set to null when the URL changes and when latest feed version is deleted
        if (latest != null && feedSource.lastFetched != null) {
            String lastUpdated = String.valueOf(Math.min(latest.updated.getTime(), feedSource.lastFetched.getTime()));
            httpGet.addHeader("If-Modified-Since", lastUpdated);
        }
        File newGtfsFile;

        try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
            httpClient.execute(httpGet);
            String message;
            StatusLine statusLine = response.getStatusLine();
            int responseCode = statusLine.getStatusCode();
            LOG.info("Fetch feed response code={}", responseCode);
            switch (responseCode) {
                case HttpURLConnection.HTTP_NOT_MODIFIED:
                    message = String.format("Feed %s has not been modified", this.name);
                    LOG.warn(message);
                    status.completeSuccessfully(message);
                    return null;
                case HttpURLConnection.HTTP_OK:
                    // Response is OK. Continue on to save the GTFS file.
                    message = String.format("Saving %s feed.", this.name);
                    LOG.info(message);
                    status.update(message, 75.0);
                    newGtfsFile = version.newGtfsFile(response.getEntity().getContent());
                    break;
                case HttpURLConnection.HTTP_MOVED_TEMP:
                case HttpURLConnection.HTTP_MOVED_PERM:
                case HttpURLConnection.HTTP_SEE_OTHER:
                    // Get redirect url from "location" header field
                    Header location = response.getFirstHeader("Location");
                    String newUrl = location != null ? location.getValue() : null;
                    if (optionalUrlOverride != null) {
                        // Only permit recursion one level deep. If more than one redirect is detected, fail the job and
                        // suggest that user try again with new URL.
                        message = String.format("More than one redirects for fetch URL detected. Please try fetch again with latest URL: %s", newUrl);
                        LOG.error(message);
                        status.fail(message);
                        return null;
                    } else {
                        // If override URL is null, this is the zeroth fetch. Recursively call fetch, but only one time
                        // to prevent multiple (possibly infinite?) redirects. Any more redirects than one should
                        // probably be met with user action to update the fetch URL.
                        LOG.info("Recursively calling fetch feed with new URL: {}", newUrl);
                        return fetch(feedSource, newUrl);
                    }
                default:
                    // Any other HTTP codes result in failure.
                    // FIXME Are there "success" codes we're not accounting for?
                    message = String.format("HTTP status (%d: %s) retrieving %s feed", responseCode, statusLine.getReasonPhrase(), this.name);
                    LOG.error(message);
                    status.fail(message);
                    return null;
            }
        } catch (IOException e) {
            String message = String.format("Unable to connect to %s; not fetching %s feed", url, this.name);
            LOG.error(message);
            status.fail(message);
            e.printStackTrace();
            return null;
        }

        // note that anything other than a new feed fetched successfully will have already returned from the function
        version.hash = HashUtils.hashFile(newGtfsFile);


        if (latest != null && version.hash.equals(latest.hash)) {
            // If new version hash equals the hash for the latest version, do not error. Simply indicate that server
            // operators should add If-Modified-Since support to avoid wasting bandwidth.
            String message = String.format("Feed %s was fetched but has not changed; server operators should add If-Modified-Since support to avoid wasting bandwidth", this.name);
            LOG.warn(message);
            String filePath = newGtfsFile.getAbsolutePath();
            if (newGtfsFile.delete()) {
                LOG.info("Deleting redundant GTFS file: {}", filePath);
            } else {
                LOG.warn("Failed to delete unneeded GTFS file at: {}", filePath);
            }
            status.completeSuccessfully(message);
            return null;
        }
        else {
            version.userId = feedSource.userId;

            // Update last fetched value for feed source.
            Persistence.feedSources.updateField(feedSource.id, "lastFetched", version.updated);

            // Set file timestamp according to last modified header from connection
            version.fileTimestamp = newGtfsFile.lastModified();
            NotifyUsersForSubscriptionJob.createNotification(
                "feed-updated",
                feedSource.id,
                String.format("New feed version created for %s.", this.name));
            String message = String.format("Fetch complete for %s", this.name);
            LOG.info(message);
            status.completeSuccessfully(message);
            return version;
        }
    }

    @Override
    public void jobLogic () {
        // TODO: fetch automatically vs. manually vs. in-house
        result = fetch(feedSource);
        // Null result indicates that a fetch was not needed (GTFS has not been modified)
        // True failures will throw exceptions.
        if (result != null) {
            // FetchSingleFeedJob should typically be run in a lightExecutor because it is a fairly lightweight task.
            // ProcessSingleFeedJob often follows a fetch and requires significant time to complete,
            // so FetchSingleFeedJob ought to be run in the heavyExecutor. Technically, the "fetch" completes
            // quickly and the "processing" happens over time. So, we run the processing in a separate thread in order
            // to match this user and system expectation.
            //
            // The exception (continueThread = true) is provided for FetchProjectFeedsJob, when we want the feeds to
            // fetch and then process in sequence.
            ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(result, this.owner, true);
            if (continueThread) {
                addNextJob(processSingleFeedJob);
            } else {
                Scheduler.runJob(feedSource.id, processSingleFeedJob);
            }
        }
    }

    @Override
    public void abortLogic() {
        // TODO Once this job is permitted to be cancelled.
        httpGet.abort();
    }

}
