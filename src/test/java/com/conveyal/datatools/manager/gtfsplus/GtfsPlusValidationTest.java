package com.conveyal.datatools.manager.gtfsplus;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.jobs.MergeFeedsJobTest;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

import static com.conveyal.datatools.TestUtils.createFeedVersionFromGtfsZip;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/** Runs test to verify that GTFS+ validation runs as expected. */
public class GtfsPlusValidationTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(MergeFeedsJobTest.class);
    private static FeedVersion bartVersion1;
    private static FeedVersion bartVersion1WithQuotedValues;
    private static Project project;

    /**
     * Create feed version for GTFS+ validation test.
     */
    @BeforeAll
    public static void setUp() throws IOException {
        // start server if it isn't already running
        DatatoolsTest.setUp();
        // Create a project, feed sources, and feed versions to merge.
        project = new Project();
        project.name = String.format("Test %s", new Date().toString());
        Persistence.projects.create(project);
        FeedSource bart = new FeedSource("BART");
        bart.projectId = project.id;
        Persistence.feedSources.create(bart);
        bartVersion1 = createFeedVersionFromGtfsZip(bart, "bart_new.zip");
        bartVersion1WithQuotedValues = createFeedVersionFromGtfsZip(bart, "bart_new_with_quoted_values.zip");
    }

    @Test
    void canValidateCleanGtfsPlus() throws Exception {
        LOG.info("Validation BART GTFS+");
        GtfsPlusValidation validation = GtfsPlusValidation.validate(bartVersion1.id);
        // Expect issues to be zero.
        assertThat("Issues count for clean BART feed is zero", validation.issues.size(), equalTo(0));
    }

    @Test
    void canValidateGtfsPlusWithQuotedValues() throws Exception {
        LOG.info("Validation BART GTFS+ with quoted values");
        GtfsPlusValidation validation = GtfsPlusValidation.validate(bartVersion1WithQuotedValues.id);
        // Expect issues to be zero.
        assertThat(
            "Issues count for clean BART feed (quoted values) is zero",
            validation.issues.size(), equalTo(0)
        );
    }
}
