package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;

/**
 * Represents a collection of feed sources that can be made into a deployment.
 * Generally, this would represent one agency that is managing the data.
 * For now, there is one FeedCollection per instance of GTFS data manager, but
 * we're trying to write the code in such a way that this is not necessary.
 *
 * @author mattwigway
 *
 */
@JsonInclude(Include.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Project extends Model {
    private static final long serialVersionUID = 1L;

    /** The name of this project, e.g. NYSDOT. */
    public String name;

    public boolean useCustomOsmBounds;

    public OtpBuildConfig buildConfig;

    public OtpRouterConfig routerConfig;

    public Collection<OtpServer> otpServers;

    public String organizationId;

    public OtpServer retrieveServer(String name) {
        for (OtpServer otpServer : otpServers) {
            if (otpServer.name.equals(name)) {
                return otpServer;
            }
        }
        return null;
    }

    public String defaultTimeZone;

    public String defaultLanguage;

    public transient Collection<FeedSource> feedSources;

    // TODO: remove default location fields (once needed for integration with gtfs-editor)
    public double defaultLocationLat, defaultLocationLon;
    public boolean autoFetchFeeds;
    public int autoFetchHour, autoFetchMinute;

    // Bounds is used for either OSM custom deployment bounds (if useCustomOsmBounds is true)
    // and/or for applying a geographic filter when syncing with external feed registries.
    public Bounds bounds;

    public Project() {
        this.buildConfig = new OtpBuildConfig();
        this.routerConfig = new OtpRouterConfig();
        this.useCustomOsmBounds = false;
    }

    /**
     * Get all the feed sources for this project.
     */
    public Collection<FeedSource> retrieveProjectFeedSources() {
        // TODO: use index, but not important for now because we generally only have one FeedCollection
        return Persistence.feedSources.getAll().stream()
                .filter(fs -> this.id.equals(fs.projectId))
                .collect(Collectors.toList());
    }

    @JsonProperty("numberOfFeeds")
    public int numberOfFeeds() {
        return retrieveProjectFeedSources().size();
    }

    /**
     * Get all the deployments for this project.
     */
    public Collection<Deployment> retrieveDeployments() {
        List<Deployment> deployments = Persistence.deployments
                .getFiltered(eq("projectId", this.id));
        return deployments;
    }

    // TODO: Does this need to be returned with JSON API response
    public Organization retrieveOrganization() {
        if (organizationId != null) {
            return Persistence.organizations.getById(organizationId);
        } else {
            return null;
        }
    }
}
