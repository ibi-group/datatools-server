package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.gtfsplus.GtfsPlusValidation;
import com.conveyal.gtfs.validator.ValidationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FeedVersionSummaryTest extends DatatoolsTest {
    @BeforeAll
    static void settingUp() throws IOException {
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
    }

    @AfterEach
    void afterEach() {
        FeedVersionSummary.setHasBlockingIssueForPublishingOverrideForTesting(null);
    }

    @ParameterizedTest
    @MethodSource("createPublishStates")
    void canDeterminePublishState(
        boolean isPublished,
        boolean isPublishing,
        boolean isPublishBlocked,
        boolean isFeedLoading,
        PublishState expectedPublishState
    ) {
        FeedSourceSummary feedSourceSummary = new FeedSourceSummary();
        FeedVersionSummary feedVersionSummary = new FeedVersionSummary();
        FeedVersion.setDateOverrideForTesting(LocalDate.now());

        if (isPublished) {
            feedVersionSummary.namespace = feedVersionSummary.feedSourcePublishedVersionId = "namespace";
        }
        if (isPublishing) {
            feedVersionSummary.sentToExternalPublisher = new Date();
            feedVersionSummary.processedByExternalPublisher = null;
        }
        if (isPublishBlocked) {
            feedVersionSummary.gtfsPlusValidation = null;
        }
        if (!isPublished && !isPublishing && !isPublishBlocked && !isFeedLoading) {
            // Ignore blocking issues.
            FeedVersionSummary.setHasBlockingIssueForPublishingOverrideForTesting(false);
            passPublishBlockedCheck(feedVersionSummary);

            feedVersionSummary.validationResult.errorCount = 1;
            feedVersionSummary.id = "feed-version-id";
            feedSourceSummary.id = "feed-source-id";
        }
        PublishState publishState = feedVersionSummary.getPublishState();
        assertEquals(expectedPublishState, publishState);
    }

    /**
     * Set up a feed version summary to pass the publish blocked check.
     */
    private static void passPublishBlockedCheck(FeedVersionSummary feedVersionSummary) {
        feedVersionSummary.gtfsPlusValidation = new GtfsPlusValidation();
        feedVersionSummary.gtfsPlusValidation.issues = new ArrayList<>();
        feedVersionSummary.gtfsPlusValidation.published = true;
        feedVersionSummary.validationResult = new ValidationResult();
        feedVersionSummary.validationResult.lastCalendarDate = LocalDate.now().plusDays(1);
        FeedVersion.setDateOverrideForTesting(LocalDate.now());
    }

    private static Stream<Arguments> createPublishStates() {
        return Stream.of(
            Arguments.of(
                true, false, false, false, PublishState.PUBLISHED
            ),
            Arguments.of(
                false, true, false, false, PublishState.PUBLISHING
            ),
            Arguments.of(
                false, false, true, false, PublishState.PUBLISH_BLOCKED
            ),
            Arguments.of(
                false, false, false, false, PublishState.READY_TO_PUBLISH
            )
        );
    }

    /**
     * Additional cases for future and expired feeds.
     */
    @ParameterizedTest
    @MethodSource("publishStateTimeCases")
    void canGetPublishState(int daysOffsetFromToday, PublishState expectedState, String caseName) {
        FeedVersionSummary feedVersionSummary = new FeedVersionSummary();
        passPublishBlockedCheck(feedVersionSummary);
        LocalDate date = LocalDate.now().plusDays(daysOffsetFromToday);
        feedVersionSummary.validationResult.firstCalendarDate = date;
        feedVersionSummary.validationResult.lastCalendarDate = date.plusDays(3);

        // Ignore blocking issues.
        FeedVersionSummary.setHasBlockingIssueForPublishingOverrideForTesting(false);
        assertEquals(expectedState, feedVersionSummary.getPublishState(), caseName);
    }

    private static Stream<Arguments> publishStateTimeCases() {
        return Stream.of(
            Arguments.of(
                1,
                PublishState.PUBLISH_BLOCKED,
                "future feed"
            ),
            Arguments.of(
                0,
                PublishState.READY_TO_PUBLISH,
                "present feed"
            ),
            Arguments.of(
                -5,
                PublishState.PUBLISH_BLOCKED,
                "expired feed"
            )
        );
    }
}
