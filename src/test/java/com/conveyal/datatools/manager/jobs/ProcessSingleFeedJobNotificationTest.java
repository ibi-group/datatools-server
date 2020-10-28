package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.gtfs.validator.ValidationResult;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;

// TODO: Remove or enhance depending on testing requirements.
public class ProcessSingleFeedJobNotificationTest extends DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessSingleFeedJobNotificationTest.class);
    private static ValidationResult validationResult = new ValidationResult();
    private MonitorableJob.Status status = new MonitorableJob.Status();

    @BeforeClass
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        LOG.info("{} setup", ProcessSingleFeedJobNotificationTest.class.getSimpleName());
        validationResult.firstCalendarDate = LocalDate.now();
        validationResult.lastCalendarDate = LocalDate.now();
    }

    @Test
    public void canNotifySubscribersOfErrors() {
        FeedVersion f = new FeedVersion();
        validationResult.errorCount = 100;
        f.validationResult = validationResult;
        status.error = false;
        String message = ProcessSingleFeedJob.getNotificationMessage("Source A", status, f);
        LOG.info("Message:" + message);
    }

    @Test
    public void canNotifySubscribersOfSuccess() {
        FeedVersion f = new FeedVersion();
        validationResult.errorCount = 0;
        f.validationResult = validationResult;
        status.error = false;
        String message = ProcessSingleFeedJob.getNotificationMessage("Source B", status, f);
        LOG.info("Message:" + message);
    }

    @Test
    public void canNotifySubscribersOfFailure() {
        FeedVersion f = new FeedVersion();
        status.error = true;
        String message = ProcessSingleFeedJob.getNotificationMessage("Source C", status, f);
        LOG.info("Message:" + message);
    }
}
