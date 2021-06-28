package com.conveyal.datatools.manager.utils;

import com.bugsnag.Bugsnag;
import com.bugsnag.Report;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ErrorUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ErrorUtils.class);

    private static Bugsnag bugsnag;

    /**
     * Create and send a report to bugsnag if configured.
     *
     * @param e The throwable object to send to Bugsnag. This MUST be provided or a report will not be generated.
     * @param userProfile An optional user profile. If provided, the email address from this profile will be set in the
     *                    Bugsnag report.
     */
    public static void reportToBugsnag(Throwable e, Auth0UserProfile userProfile) {
        reportToBugsnag(e, null, userProfile);
    }

    /**
     * Create and send a report to bugsnag if configured.
     *
     * @param e The throwable object to send to Bugsnag. This MUST be provided or a report will not be generated.
     * @param debuggingMessages An optional Map of keys and values that will be added to the `debugging` tab within the
     *                          Bugsnag report.
     * @param userProfile An optional user profile. If provided, the email address from this profile will be set in the
     *                    Bugsnag report.
     */
    public static void reportToBugsnag(
        Throwable e, Map<String, String> debuggingMessages, Auth0UserProfile userProfile
    ) {
        if (bugsnag == null) {
            LOG.warn("Bugsnag not configured to report errors!");
            return;
        }
        if (e == null) {
            LOG.warn("No Throwable object provided. A Bugsnag report will not be created!");
            return;
        }

        // create report to send to bugsnag
        Report report = bugsnag.buildReport(e);
        String userEmail = userProfile != null ? userProfile.getEmail() : "no-auth";
        report.setUserEmail(userEmail);
        if (debuggingMessages != null) {
            debuggingMessages.entrySet().stream()
                .forEach((Map.Entry<String, String> entry) -> report.addToTab(
                    "debugging",
                    entry.getKey(),
                    entry.getValue()
                ));
        }
        bugsnag.notify(report);
    }

    // intialize bugsnag
    public static void initialize() {
        String bugsnagKey = DataManager.getConfigPropertyAsText("BUGSNAG_KEY");
        if (bugsnagKey != null) {
            bugsnag = new Bugsnag(bugsnagKey);
        }
    }
}