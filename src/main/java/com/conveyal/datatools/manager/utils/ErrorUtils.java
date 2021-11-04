package com.conveyal.datatools.manager.utils;

import com.bugsnag.Bugsnag;
import com.bugsnag.Report;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A util class for reporting errors to the project defined by the Bugsnag project notifier API key.
 *
 * A Bugsnag project identifier key is unique to a Bugsnag project and allows errors to be saved against it. This key
 * can be obtained by logging into Bugsnag (https://app.bugsnag.com), clicking on Projects (left side menu) and
 * selecting the required project. Once selected, the notifier API key is presented.
 */
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
     * Log an error, and create and send a report to bugsnag if configured.
     *
     * @param e The throwable object to send to Bugsnag. This MUST be provided or a report will not be generated.
     * @param sourceApp The application generating the message (datatools, otp-runner, ...).
     * @param message The message to log and to send to Bugsnag.
     * @param userProfile An optional user profile. If provided, the email address from this profile will be set in the
     *                    Bugsnag report.
     */
    public static void reportToBugsnag(Throwable e, String sourceApp, String message, Auth0UserProfile userProfile) {
        Map<String, String> debuggingMessages = new HashMap<>();
        debuggingMessages.put(sourceApp + " message", message);
        reportToBugsnag(e, debuggingMessages, userProfile);
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

    /**
     * Initialize Bugsnag reporting if a Bugsnag key exists in the configuration.
     */
    public static void initialize() {
        String bugsnagKey = DataManager.getConfigPropertyAsText("BUGSNAG_KEY");
        if (bugsnagKey != null) {
            bugsnag = new Bugsnag(bugsnagKey);
        }
    }
}
