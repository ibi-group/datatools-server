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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
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
    private static final long SESSION_LENGTH_IN_SECONDS = 60 * 60; // One hour


    private static String lockFeed (Request req, Response res) {
        // FIXME: why is content type not being set in before()/after()?
        res.type("application/json");
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
            // FIXME: Should there be a user action to "boot" the other session lock?
            // Create new session
            String newSessionId = invalidateAndCreateNewSession(req);
            LOG.info("User {} (session ID: {}) has not maintained lock for {} minutes. Booting.", currentSession.userEmail, currentSession.sessionId, minutesSinceLastCheckIn);
            EditorSession newEditorSession = new EditorSession(feedId, newSessionId, userProfile);
            sessionsForFeedIds.put(feedId, newEditorSession);
            return formatJSON("Locking editor feed for user " + newEditorSession.userEmail, 200, feedId, newSessionId);
        } else if (!currentSession.userId.equals(userProfile.getUser_id())) {
            // If the session has not expired, and another user has the active session.
            LOG.warn("Edit session {} for user {} in progress for feed {}. User {} not permitted to lock feed for {} minutes.", currentSession.sessionId, currentSession.userEmail, currentSession.feedId, userProfile.getEmail(), minutesUntilExpiration);
            String message = String.format(
                    "Warning! There is an editor session already in progress for user %s. " +
                            "Their session will expire in %d minutes unless they are actively editing.",
                    currentSession.userEmail,
                    minutesUntilExpiration);
            haltWithMessage(400, message);
            return null;
        } else {
            String sessionId = req.session().id();
            LOG.warn("User {} is editing feed {} in another session {}. Cannot create lock for session {}", userProfile.getEmail(), feedId, currentSession.sessionId, sessionId);
            haltWithMessage(400, "Warning! You are editing this feed in another session/browser tab!");
            return null;
        }
    }

    private static String invalidateAndCreateNewSession(Request req) {
        req.session().invalidate();
        Session session = req.session(true);
        String newSessionId = session.id();
        return newSessionId;
    }

    private static String maintainLock(Request req, Response res) {
        // FIXME: why is content type not being set in before()/after()?
        res.type("application/json");
        String sessionId = req.params("id");
        String feedId = req.queryParams("feedId");
        Auth0UserProfile userProfile = req.attribute("user");
        EditorSession currentSession = sessionsForFeedIds.get(feedId);
        if (currentSession == null) {
            // If there is no current session to maintain, request that user reloads browser.
            LOG.warn("No active editor session to maintain {}.", sessionId);
            haltWithMessage(400, "No active session for feedId. Please refresh your browser and try editing later.");
            return null;
        } else if (!currentSession.sessionId.equals(sessionId)) {
            // If there is an active session but it doesn't match the session, someone else (or the current user) is
            // editing elsewhere. A user should only be trying to maintain a lock if it had an active session at one
            // point. If we get to this point, it is because the user's session has expired and some other session took
            // its place.
            if (currentSession.userEmail.equals(userProfile.getEmail())) {
                // If the new current session is held by this user, give them the option to evict the current session /
                // unlock the feed.
                LOG.warn("User {} already has an active editor session () for feed {}.", userProfile.getEmail(), currentSession.sessionId, currentSession.feedId);
                haltWithMessage(400, "Warning! You have an active editing session for this feed underway in a different browser tab.");
            } else {
                // FIXME: Is it bad to reveal the user email? No, I don't think so. Users have already been authenticated and
                // must have permissions on the feed source to even get to this point.
                LOG.warn("User {} attempted editor session for feed {} while active session underway for user {}.", userProfile.getEmail(), currentSession.feedId, currentSession.userEmail);
                haltWithMessage(400, "Warning! There is an editor session underway for this feed. User = " + currentSession.userEmail);
            }
            return null;
        } else {
            // Otherwise, the current session matches the session the user is attempting to maintain. Update the
            // lastEdited time.
            currentSession.lastCheckIn = System.currentTimeMillis();
//            LOG.info("Updating session {} check-in time to {} for user {}", currentSession.sessionId, currentSession.lastCheckIn, currentSession.userEmail);
            return formatJSON("Updating time for user " + currentSession.userEmail, 200, feedId, null);
        }
    }

    private static String deleteFeedLock(Request req, Response res) {
        // FIXME: why is content type not being set in before()/after()?
        res.type("application/json");
        Auth0UserProfile userProfile = req.attribute("user");
        String feedId = req.queryParams("feedId");
        String sessionId = req.params("id");
        EditorSession currentSession = sessionsForFeedIds.get(feedId);
        if (currentSession == null) {
            // If there is no current session to delete/overwrite, request that user reloads browser.
            LOG.warn("No active session to overwrite/delete.");
            return SparkUtils.formatJSON("No active session to take over. Please refresh your browser and try editing later.", 202);
        } else if (!currentSession.sessionId.equals(sessionId)) {
            if (currentSession.userEmail.equals(userProfile.getEmail())) {
                // If there is a different active session for the current user, allow deletion / overwrite.
                boolean overwrite = Boolean.valueOf(req.queryParams("overwrite"));
                if (overwrite) {
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
                // If there is a different active session for some other user, prevent deletion.
                LOG.warn("User {} not permitted to overwrite active session {} for user {}", userProfile.getEmail(), currentSession.sessionId, currentSession.userEmail);
                return SparkUtils.formatJSON("Cannot overwrite session for user " + currentSession.userEmail, 202);
            }
        } else {
            LOG.info("Current session: {} {}; User session: {} {}", currentSession.userEmail, currentSession.sessionId, userProfile.getEmail(), sessionId);
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
