package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;

/**
 * Represents a collection of feed sources that can be made into a deployment.
 * Generally, this would represent one agency that is managing the data.
 * For now, there is one Project per instance of GTFS data manager, but
 * we're trying to write the code in such a way that this is not necessary.
 *
 * @author mattwigway
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Project extends Model {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Project.class);

    /** The name of this project, e.g. NYSDOT. */
    public String name;

    public boolean useCustomOsmBounds;

    public OtpBuildConfig buildConfig;

    public OtpRouterConfig routerConfig;

    public String organizationId;

    /**
     * A list of servers that are available to deploy project feeds/OSM to. This includes servers assigned to this
     * project as well as those that belong to no project.
     * @return
     */
    @JsonProperty("otpServers")
    public List<OtpServer> availableOtpServers() {
        return Persistence.servers.getFiltered(or(
            eq("projectId", this.id),
            eq("projectId", null)
        ));
    }

    public String defaultTimeZone;
    public boolean autoFetchFeeds;
    public int autoFetchHour, autoFetchMinute;

    public transient Collection<FeedSource> feedSources;

    // Bounds is used for either OSM custom deployment bounds (if useCustomOsmBounds is true)
    // and/or for applying a geographic filter when syncing with external feed registries.
    public Bounds bounds;

    // Identifies a specific "pinned" deployment for the project. This is used in datatools-ui in 2 places:
    // 1. In the list of project deployments, a "pinned" deployment is shown first and highlighted.
    // 2. In the project feed source table, if a "pinned" deployment exists, the status of the versions that were in
    //   the "pinned" deployment are shown and compared to the most recent version in the feed sources.
    public String pinnedDeploymentId;

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

    // Note: Previously a numberOfFeeds() dynamic Jackson JsonProperty was in place here. But when the number of projects
    // in the database grows large, the efficient calculation of this field does not scale.

    /**
     * Get all the deployments for this project.
     */
    public Collection<Deployment> retrieveDeployments() {
        return Persistence.deployments.getFiltered(eq("projectId", this.id));
    }

    // TODO: Does this need to be returned with JSON API response
    public Organization retrieveOrganization() {
        if (organizationId != null) {
            return Persistence.organizations.getById(organizationId);
        } else {
            return null;
        }
    }

    public void delete() {
        // FIXME: Handle this in a Mongo transaction. See https://docs.mongodb.com/master/core/transactions/#transactions-and-mongodb-drivers
        // Delete each feed source in the project (which in turn deletes each feed version).
        retrieveProjectFeedSources().forEach(FeedSource::delete);
        // Delete each deployment in the project.
        retrieveDeployments().forEach(Deployment::delete);
        // Finally, delete the project.
        Persistence.projects.removeById(this.id);
    }
}
