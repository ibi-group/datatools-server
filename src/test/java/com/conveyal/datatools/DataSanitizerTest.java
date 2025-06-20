package com.conveyal.datatools;

import com.conveyal.datatools.common.utils.aws.CheckedAWSException;
import com.conveyal.datatools.manager.DataSanitizer;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.mongodb.client.model.Sorts;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import static com.conveyal.datatools.TestUtils.appendDate;
import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static com.conveyal.datatools.manager.models.FeedRetrievalMethod.MANUALLY_UPLOADED;
import static com.mongodb.client.model.Filters.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataSanitizerTest {

    private static Project project;
    private static FeedVersion feedVersionOrphan;
    private static FeedVersion feedVersionWithParent;
    private static FeedSource feedSourceParent;
    private static FeedSource feedSourceWithObsoleteFeedVersion;
    @BeforeAll
    static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        ProcessSingleFeedJob.VALIDATE_MOBILITY_DATA = false;
        project = new Project();
        project.name = appendDate("Test");
        Persistence.projects.create(project);
        FeedSource feedSource = new FeedSource(appendDate("Test Feed"), project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(feedSource);
        feedSourceParent = new FeedSource(appendDate("Test Feed 2"), project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(feedSourceParent);
        feedSourceWithObsoleteFeedVersion = new FeedSource(appendDate("Test Feed 3"), project.id, MANUALLY_UPLOADED);
        Persistence.feedSources.create(feedSourceWithObsoleteFeedVersion);
        feedVersionOrphan = createFeedVersion(
            feedSource,
            zipFolderFiles("fake-agency-with-only-calendar")
        );
        feedVersionWithParent = createFeedVersion(
            feedSourceParent,
            zipFolderFiles("fake-agency-with-only-calendar")
        );
        // Delete feed source to orphan feed version.
        Persistence.feedSources.removeById(feedSource.id);

        for(int i=0; i<4; i++){
            createFeedVersion(
                feedSourceWithObsoleteFeedVersion,
                zipFolderFiles("fake-agency-with-only-calendar")
            );
        }

    }

    @AfterAll
    static void tearDown() throws SQLException, InvalidNamespaceException, CheckedAWSException {
        ProcessSingleFeedJob.VALIDATE_MOBILITY_DATA = false;
        Auth0Connection.setAuthDisabled(false);
        project.delete();
        FeedVersion feedVersion = Persistence.feedVersions.getById(feedVersionOrphan.id);
        if (feedVersion != null) {
            feedVersionOrphan.deleteOrphan();
        }
    }

    @Test
    void canIdentifyOrphanedFeedVersion() {
        assertEquals(1, DataSanitizer.sanitizeOrphanedFeedVersions(false));
        assertNotNull(Persistence.feedVersions.getById(feedVersionOrphan.id));
        assertNotNull(Persistence.feedVersions.getById(feedVersionWithParent.id));
    }

    @Test
    void canRemoveOrphanedFeedVersion() {
        assertEquals(1, DataSanitizer.sanitizeOrphanedFeedVersions(true));
        assertNull(Persistence.feedVersions.getById(feedVersionOrphan.id));
        assertNotNull(Persistence.feedVersions.getById(feedVersionWithParent.id));
    }

    @Test
    void canAuditFeedVersions() {
        Map<String, Integer> audit = DataSanitizer.feedVersionAudit();
        assertEquals(1, audit.get(feedSourceParent.id));
    }

    @Test
    void canSanitizeObsoleteFeedVersions() {
        // From the initial four feed versions, delete the older two.
        int deleted = DataSanitizer.sanitizeObsoleteFeedVersions(
            feedSourceWithObsoleteFeedVersion.id,
            2
        );
        assertEquals(2, deleted);
        Collection<FeedVersion> feedVersions = Persistence.feedVersions.getFiltered(
            eq("feedSourceId", feedSourceWithObsoleteFeedVersion.id),
            Sorts.descending("version")
        );
        // Feed versions are renumbered after being deleted. Should no longer have versions three and four.
        for(FeedVersion feedVersion : feedVersions){
            assertTrue(feedVersion.version <= 2);
        }
    }
}