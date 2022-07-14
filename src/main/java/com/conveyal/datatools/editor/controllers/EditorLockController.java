package com.conveyal.datatools.editor.controllers;

import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Session;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static spark.Spark.delete;
import static spark.Spark.post;
import static spark.Spark.put;

/**
 * Handles the locking of feed sources being edited to prevent concurrent feed editing. In addition to locking against
 * concurrent editing by more than one user, this restricts editing by a single user on multiple sessions/tabs.
 */
public class EditorLockController {

    private static final JsonManager<EditorLockController> json = new JsonManager<>(EditorLockController.class, JsonViews.UserInterface.class);
    private static final Logger LOG = LoggerFactory.getLogger(EditorLockController.class);
    public static final Map<String, EditorSession> sessionsForFeedIds = new HashMap<>();
    private static final long SESSION_LENGTH_IN_SECONDS = 10 * 60; // Ten minutes


    private static String lockFeed (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        String feedId = req.queryParams("feedId");
        EditorSession currentSession = sessionsForFeedIds.get(feedId);
        if (currentSession == null) {
            // If there is no active session for the feed ID, create a new one, which allows only the current user +
            // session to edit.
            // Create new session
            String newSessionId = invalidateAndCreateNewSession(req);
            EditorSession newEditorSession = new EditorSession(feedId, newSessionId, userProfile);
            sessionsForFeedIds.put(feedId, newEditorSession);
            LOG.info("Locking feed {} for editing session {} by user {}", feedId, newSessionId, userProfile.getEmail());
            return formatJSON("Locking editor feed for user " + newEditorSession.userEmail,
                    200,
                    feedId,
                    newSessionId);
        }

        long secondsSinceLastCheckIn = TimeUnit.MILLISECONDS.toSeconds  (System.currentTimeMillis() - currentSession.lastCheckIn);
        long minutesSinceLastCheckIn = TimeUnit.SECONDS.toMinutes(secondsSinceLastCheckIn);
        long minutesUntilExpiration = TimeUnit.SECONDS.toMinutes(SESSION_LENGTH_IN_SECONDS - secondsSinceLastCheckIn);
        if (secondsSinceLastCheckIn > SESSION_LENGTH_IN_SECONDS) {
            // There is an active session, but the user with active session has not checked in for some time. Booting
            // the current session in favor of new session.
            // Create new session
            String newSessionId = invalidateAndCreateNewSession(req);
            LOG.info("User {} (session ID: {}) has not maintained lock for {} minutes. Booting.", currentSession.userEmail, currentSession.sessionId, minutesSinceLastCheckIn);
            EditorSession newEditorSession = new EditorSession(feedId, newSessionId, userProfile);
            sessionsForFeedIds.put(feedId, newEditorSession);
            return formatJSON("Locking editor feed for user " + newEditorSession.userEmail, 200, feedId, newSessionId);
        } else if (!currentSession.userId.equals(userProfile.getUser_id())) {
            // If the session has not expired, and another user has the active session.
            LOG.warn("Edit session {} for user {} in progress for feed {}. User {} not permitted to lock feed for {} minutes.", currentSession.sessionId, currentSession.userEmail, currentSession.feedId, userProfile.getEmail(), minutesUntilExpiration);
            logMessageAndHalt(req, 400, getLockedFeedMessage(currentSession, minutesUntilExpiration));
            return null;
        } else {
            String sessionId = req.session().id();
            LOG.warn("User {} is editing feed {} in another session {}. Cannot create lock for session {}", userProfile.getEmail(), feedId, currentSession.sessionId, sessionId);
            logMessageAndHalt(req, 400, "Warning! You are editing this feed in another session/browser tab!");
            return null;
        }
    }

    private static String getLockedFeedMessage(EditorSession session, long minutesUntilExpiration) {
        String timestamp = session.lastEdit > 0
                ? SimpleDateFormat.getInstance().format(new Date(session.lastEdit))
                : null;
        String lastEditMessage = timestamp == null ? "no edits since session began" : "last edit was " + timestamp;
        return String.format(
                "Warning! There is an editor session already in progress for user %s. " +
                        "Their session will expire after %d minutes of inactivity (%s).",
                session.userEmail,
                minutesUntilExpiration,
                lastEditMessage);
    }

    private static String invalidateAndCreateNewSession(Request req) {
        req.session().invalidate();
        Session session = req.session(true);
        return session.id();
    }

    private static String maintainLock(Request req, Response res) {
        String sessionId = req.params("id");
        String feedId = req.queryParams("feedId");
        Auth0UserProfile userProfile = req.attribute("user");
        EditorSession currentSession = sessionsForFeedIds.get(feedId);
        if (currentSession == null) {
            // If there is no current session to maintain, request that user reloads browser.
            LOG.warn("No active editor session to maintain {}.", sessionId);
            logMessageAndHalt(req, 400, "No active session for feedId. Please refresh your browser and try editing later.");
            return null;
        } else if (!currentSession.sessionId.equals(sessionId)) {
            long secondsSinceLastCheckIn = TimeUnit.MILLISECONDS.toSeconds  (System.currentTimeMillis() - currentSession.lastCheckIn);
            long minutesUntilExpiration = TimeUnit.SECONDS.toMinutes(SESSION_LENGTH_IN_SECONDS - secondsSinceLastCheckIn);
            // If there is an active session but it doesn't match the session, someone else (or the current user) is
            // editing elsewhere. A user should only be trying to maintain a lock if it had an active session at one
            // point. If we get to this point, it is because the user's session has expired and some other session took
            // its place.
            if (currentSession.userEmail.equals(userProfile.getEmail())) {
                // If the new current session is held by this user, give them the option to evict the current session /
                // unlock the feed.
                LOG.warn("User {} already has an active editor session {} for feed {}.", userProfile.getEmail(), currentSession.sessionId, currentSession.feedId);
                logMessageAndHalt(req, 400, "Warning! You have an active editing session for this feed underway in a different browser tab.");
            } else {
                LOG.warn("User {} attempted editor session for feed {} while active session underway for user {}.", userProfile.getEmail(), currentSession.feedId, currentSession.userEmail);
                logMessageAndHalt(req, 400, getLockedFeedMessage(currentSession, minutesUntilExpiration));
            }
            return null;
        } else {
            // Otherwise, the current session matches the session the user is attempting to maintain. Update the
            // lastEdited time.
            currentSession.lastCheckIn = System.currentTimeMillis();
            return formatJSON("Updating time for user " + currentSession.userEmail, 200, feedId, null);
        }
    }

    /**
     * Normal path for deleting a feed lock.
     */
    private static String deleteFeedLock(Request req, Response res) {
        return deleteFeedLockCore(req, req.attribute("user"));
    }

    /**
     * Remove a feed lock when a browser calls sendBeacon() when closing/refreshing/navigating away from editor.
     */
    private static String deleteFeedLockBeacon(Request req, Response res) {
        // The sendBeacon call does not contain any Authorization headers, so we just pass a null userProfile.
        return deleteFeedLockCore(req, null);
    }

    private static String deleteFeedLockCore(Request req, Auth0UserProfile userProfile) {
        String feedId = req.queryParams("feedId");
        String sessionId = req.params("id");
        EditorSession currentSession = sessionsForFeedIds.get(feedId);
        if (currentSession == null) {
            // If there is no current session to delete/overwrite, request that user reloads browser.
            LOG.warn("No active session to overwrite/delete.");
            return SparkUtils.formatJSON("No active session to take over. Please refresh your browser and try editing later.", 202);
        } else if (!currentSession.sessionId.equals(sessionId)) {
            // If there is a different active session for some user, allow deletion / overwrite.
            // Note: There used to be a check here that the requesting user was the same as the user with an open
            // session; however, this has been removed because in practice it became a nuisance. Respectful users with
            // shared access to a feed can generally be trusted not to boot one another out in a combative manner.
            boolean overwrite = Boolean.parseBoolean(req.queryParams("overwrite"));
            if (userProfile != null && overwrite) {
                sessionId = invalidateAndCreateNewSession(req);
                EditorSession newEditorSession = new EditorSession(feedId, sessionId, userProfile);
                sessionsForFeedIds.put(feedId, newEditorSession);
                LOG.warn("Previously active session {} has been overwritten with new session {}.", currentSession.sessionId, newEditorSession.sessionId);
                return formatJSON("Previous session lock has been overwritten with new session.", 200, feedId, sessionId);
            } else {
                LOG.warn("Not overwriting session {} for user {}.", currentSession.sessionId, currentSession.userEmail);
                return SparkUtils.formatJSON("Not processing request to delete lock. There is already an active session for user " + currentSession.userEmail, 202);
            }
        } else {
            LOG.info(
                "Current session: {} {}; User session: {} {}",
                currentSession.userEmail,
                currentSession.sessionId,
                userProfile != null ? userProfile.getEmail() : "(email unavailable)",
                sessionId
            );
            // Otherwise, the current session matches the session from which the delete request came. This indicates that
            // the user's editing session has been closed (by either exiting the editor or closing the browser tab).
            LOG.info("Closed session {} for feed {} successfully.", currentSession.sessionId, currentSession.feedId);
            sessionsForFeedIds.remove(feedId);
            return formatJSON("Session has been closed successfully.", 200, feedId, sessionId);
        }
    }

    public static void register(String apiPrefix) {
        post(apiPrefix + "secure/lock", EditorLockController::lockFeed, json::write);
        delete(apiPrefix + "secure/lock/:id", EditorLockController::deleteFeedLock, json::write);
        put(apiPrefix + "secure/lock/:id", EditorLockController::maintainLock, json::write);
        // Extra, unsecure POST method for removing lock via a browser's Navigator.sendBeacon() method.
        // (Navigator.sendBeacon() sends a POST and does not support authorization headers.)
        post(apiPrefix + "deletelock/:id", EditorLockController::deleteFeedLockBeacon, json::write);
    }

    private static String formatJSON(String message, int code, String feedId, String sessionId) {
        JsonObject object = new JsonObject();
        object.addProperty("result", code >= 400 ? "ERR" : "OK");
        object.addProperty("message", message);
        object.addProperty("code", code);
        if (sessionId != null) {
            object.addProperty("sessionId", sessionId);
        }
        object.addProperty("feedId", feedId);
        return object.toString();
    }

    public static class EditorSession {
        public final String feedId;
        public final String sessionId;
        public final String userId;
        public final String userEmail;
        public long lastCheckIn;
        public long lastEdit;

        EditorSession (String feedId, String sessionId, Auth0UserProfile userProfile) {
            this.feedId = feedId;
            this.sessionId = sessionId;
            this.userId = userProfile != null ? userProfile.getUser_id() : "no_user_id";
            this.userEmail = userProfile != null ? userProfile.getEmail() : "no_user_email";
            lastCheckIn = System.currentTimeMillis();
        }
    }
}
