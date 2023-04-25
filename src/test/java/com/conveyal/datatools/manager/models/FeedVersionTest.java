package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.NewGTFSErrorType;
import com.conveyal.gtfs.error.SQLErrorStorage;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.conveyal.gtfs.validator.ValidationResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;

import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static com.conveyal.datatools.manager.DataManager.GTFS_DATA_SOURCE;
import static com.mongodb.client.model.Filters.eq;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class FeedVersionTest extends UnitTest {
    private static Project project;
    private static FeedSource feedSource;

    /** Initialize application for tests to run. */
    @BeforeAll
    public static void setUp() throws Exception {
        // start server if it isn't already running
        DatatoolsTest.setUp();

        // set up project
        project = new Project();
        project.name = String.format("Test project %s", new Date());
        Persistence.projects.create(project);

        feedSource = new FeedSource("Test feed source");
        feedSource.projectId = project.id;
        Persistence.feedSources.create(feedSource);
    }

    @AfterAll
    public static void tearDown() {
        if (project != null) {
            project.delete();
        }
    }

    /**
     * Make sure FeedVersionIDs are always unique, even if created at the same second.
     * See https://github.com/ibi-group/datatools-server/issues/251
     */
    @Test
    void canCreateUniqueFeedVersionIDs() {
        // Create a project, feed sources, and feed versions to merge.
        // create two feedVersions immediately after each other which should end up having unique IDs
        FeedVersion feedVersion1 = new FeedVersion(feedSource);
        FeedVersion feedVersion2 = new FeedVersion(feedSource);
        assertThat(feedVersion1.id, not(equalTo(feedVersion2.id)));
    }

    /**
     * Detect feeds with fatal exceptions (a blocking issue for publishing).
     */
    @Test
    void canDetectBlockingFatalExceptionsForPublishing() {
        FeedVersion feedVersion1 = new FeedVersion(feedSource);
        feedVersion1.validationResult = new ValidationResult();
        feedVersion1.validationResult.fatalException = "A fatal exception occurred";

        assertThat(feedVersion1.hasBlockingIssuesForPublishing(), equalTo(true));
    }

    /**
     * Detect feeds with blocking error types that prevents publishing, per
     * https://github.com/ibi-group/datatools-ui/blob/dev/lib/manager/util/version.js#L79.
     */
    @ParameterizedTest
    @EnumSource(value = NewGTFSErrorType.class, names = { 
        "ILLEGAL_FIELD_VALUE",
        "MISSING_COLUMN",
        "REFERENTIAL_INTEGRITY",
        "SERVICE_WITHOUT_DAYS_OF_WEEK",
        "TABLE_MISSING_COLUMN_HEADERS",
        "TABLE_IN_SUBDIRECTORY",
        "WRONG_NUMBER_OF_FIELDS"
    })
    void canDetectBlockingErrorTypesForPublishing(NewGTFSErrorType errorType) throws InvalidNamespaceException, SQLException {
        FeedVersion feedVersion1 = createFeedVersionFromGtfsZip(feedSource, "bart_old_lite.zip");

        // Add blocking error types to feed version
        try (Connection connection = GTFS_DATA_SOURCE.getConnection()) {
            SQLErrorStorage errorStorage = new SQLErrorStorage(connection, feedVersion1.namespace + ".", false);
            errorStorage.storeError(NewGTFSError.forFeed(errorType, null));
            errorStorage.commitAndClose();
        }

        assertThat(feedVersion1.hasBlockingIssuesForPublishing(), equalTo(true));
    }

    /**
     * {@link FeedSource::publishedVersionId} should be unset if it references a feed version being deleted.
     */
    @Test
    void shouldDeletePublishedVersionIdWhenDeletingVersion() {
        final String NAMESPACE = "published_namespace";

        FeedSource storedFeedSource = Persistence.feedSources.getOneFiltered(eq("projectId", project.id));
        String feedSourceId = storedFeedSource.id;

        // Create a feed version linked to the feed source.
        FeedVersion feedVersion1 = new FeedVersion(feedSource);
        feedVersion1.namespace = NAMESPACE;
        feedVersion1.feedSourceId = feedSourceId;
        Persistence.feedVersions.create(feedVersion1);

        // Other feed version linked to the feed source, without effect to the published version.
        FeedVersion feedVersion2 = new FeedVersion(feedSource);
        feedVersion2.namespace = "other_namespace";
        feedVersion2.feedSourceId = feedSourceId;
        Persistence.feedVersions.create(feedVersion2);

        // Set publishedVersionId of FeedSource as the namespace of the feed version, per FeedVersionController logic.
        Persistence.feedSources.updateField(feedSourceId, "publishedVersionId", NAMESPACE);

        // publishedVersionId should be set at this point.
        assertThat(getPubVersionId(feedSourceId), equalTo(NAMESPACE));

        // Deleting feedVersion2 should not touch publishedVersionId.
        feedVersion2.delete();
        assertThat(getPubVersionId(feedSourceId), equalTo(NAMESPACE));

        // Deleting feedVersion1 should unset publishedVersionId.
        feedVersion1.delete();
        assertThat(getPubVersionId(feedSourceId), equalTo(null));

        // The delete statements above will not remove the feed versions from Mongo.
        // This is because of expected caught exceptions about deleting the non-existent namespaces in this test.
        assertThat(Persistence.feedVersions.getById(feedVersion1.id), notNullValue());
        assertThat(Persistence.feedVersions.getById(feedVersion2.id), notNullValue());
        // Delete the feed versions manually as a result.
        Persistence.feedVersions.removeById(feedVersion1.id);
        Persistence.feedVersions.removeById(feedVersion2.id);
    }

    String getPubVersionId(String feedSourceId) {
        return Persistence.feedSources.getById(feedSourceId).publishedVersionId;
    }
}
