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
    private static final Map<String, EditorSession> sessionsForFeedIds = new HashMap<>();
    private static final long SESSION_LENGTH_IN_SECONDS = 10 * 60L; // Ten minutes

    /**
     * Returns the current session based on the info provided.
     */
    public static EditorSession getCurrentSession(ParsedRequest req) {
        return sessionsForFeedIds.get(req.getSessionKey());
    }

    /**
     * Returns a session based on a feed id.
     */
    public static EditorSession getSession(String feedId) {
        return sessionsForFeedIds.values().stream()
            .filter(s -> s.feedId.equals(feedId))
            .findFirst()
            .orElse(null);
    }

    private static String lockFeed (Request req, Response res) {
        ParsedRequest parsedReq = new ParsedRequest(req);
        EditorSession currentSession = getCurrentSession(parsedReq);
        String email = parsedReq.userProfile.getEmail();
        if (currentSession == null) {
            // If there is no active session for the feed ID, create a new one, which allows only the current user +
            // session to edit.
            return invalidateAndCreateNewSession(
                parsedReq,
                String.format("Locking feed for user %s on %s", email, parsedReq.itemToLock),
                String.format("Locking feed %s for user %s on %s", parsedReq.feedId, email, parsedReq.itemToLock)
            );
        }

        long secondsSinceLastCheckIn = currentSession.secondsSinceLastCheckIn();
        if (secondsSinceLastCheckIn > SESSION_LENGTH_IN_SECONDS) {
            // There is an active session, but the user with active session has not checked in for some time. Booting
            // the current session in favor of new session.
            long minutesSinceLastCheckIn = TimeUnit.SECONDS.toMinutes(secondsSinceLastCheckIn);
            return invalidateAndCreateNewSession(
                parsedReq,
                String.format("Locking feed for user %s on %s", email, parsedReq.itemToLock),
                String.format("User %s has not maintained lock for %d minutes. Booting", email, minutesSinceLastCheckIn)
            );
        } else if (!currentSession.userId.equals(parsedReq.userProfile.getUser_id())) {
            // If the session has not expired, and another user has the active session.
            LOG.warn(
                "Edit session {} for user {} in progress for feed {}. User {} not permitted to lock feed for {} minutes.",
                currentSession.sessionId,
                currentSession.userEmail,
                currentSession.feedId,
                email,
                currentSession.minutesUntilExpiration());
            logMessageAndHalt(req, 400, getLockedFeedMessage(currentSession));
            return null;
        } else {
            String sessionId = req.session().id();
            LOG.warn(
                "User {} is editing feed {} in another session {}. Cannot create lock for session {}",
                email,
                parsedReq.feedId,
                currentSession.sessionId,
                sessionId
            );
            logMessageAndHalt(req, 400, "Warning! You are editing this feed in another session/browser tab!");
            return null;
        }
    }

    private static String invalidateAndCreateNewSession(ParsedRequest req, String message, String logMessage) {
        req.request.session().invalidate();
        Session session = req.request.session(true);
        String newSessionId = session.id();

        EditorSession newEditorSession = new EditorSession(req.feedId, newSessionId, req.userProfile, req.itemToLock);
        sessionsForFeedIds.put(req.getSessionKey(), newEditorSession);
        LOG.info("{} (Session ID: {})", logMessage, newSessionId);
        return formatSuccessJSON(message, req.feedId, newSessionId);
    }

    private static String getLockedFeedMessage(EditorSession session) {
        String timestamp = session.lastEdit > 0
                ? SimpleDateFormat.getInstance().format(new Date(session.lastEdit))
                : null;
        String lastEditMessage = timestamp == null ? "no edits since session began" : "last edit was " + timestamp;
        return String.format(
                "Warning! There is an editor session already in progress for user %s. " +
                        "Their session will expire after %d minutes of inactivity (%s).",
                session.userEmail,
                session.minutesUntilExpiration(),
                lastEditMessage);
    }

    public static boolean checkUserHasActiveSession(Request req, String sessionId, String email, EditorSession currentSession) {
        if (currentSession == null) {
            // If there is no current session to maintain, request that user reloads browser.
            LOG.warn("No active editor session to maintain {}.", sessionId);
            logMessageAndHalt(req, 400, "No active session for feedId. Please refresh your browser and try editing later.");
            return false;
        } else if (!currentSession.sessionId.equals(sessionId)) {
            // If there is an active session but it doesn't match the session, someone else (or the current user) is
            // editing elsewhere. A user should only be trying to maintain a lock if it had an active session at one
            // point. If we get to this point, it is because the user's session has expired and some other session took
            // its place.
            if (currentSession.userEmail.equals(email)) {
                // If the new current session is held by this user, give them the option to evict the current session /
                // unlock the feed.
                LOG.warn("User {} already has an active editor session {} for feed {}.", email, currentSession.sessionId, currentSession.feedId);
                logMessageAndHalt(req, 400, "Warning! You have an active editing session for this feed underway in a different browser tab.");
            } else {
                LOG.warn("User {} attempted editor session for feed {} while active session underway for user {}.", email, currentSession.feedId, currentSession.userEmail);
                logMessageAndHalt(req, 400, getLockedFeedMessage(currentSession));
            }
            return false;
        }
        return true;
    }

    private static String maintainLock(Request req, Response res) {
        ParsedRequest parsedReq = new ParsedRequest(req);
        EditorSession currentSession = getCurrentSession(parsedReq);
        if (checkUserHasActiveSession(req, parsedReq.sessionId, parsedReq.userProfile.getEmail(), currentSession)) {
            // If the current session matches the session the user is attempting to maintain. Update the
            // lastEdited time.
            currentSession.lastCheckIn = System.currentTimeMillis();
            return formatSuccessJSON("Updating time for user " + currentSession.userEmail, parsedReq.feedId, null);
        } else {
            return null;
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
        ParsedRequest parsedReq = new ParsedRequest(req);
        EditorSession currentSession = getCurrentSession(parsedReq);
        if (currentSession == null) {
            // If there is no current session to delete/overwrite, request that user reloads browser.
            LOG.warn("No active session to overwrite/delete.");
            return SparkUtils.formatJSON("No active session to take over. Please refresh your browser and try editing later.", 202);
        } else if (!currentSession.sessionId.equals(parsedReq.sessionId)) {
            // If there is a different active session for some user, allow deletion / overwrite.
            // Note: There used to be a check here that the requesting user was the same as the user with an open
            // session; however, this has been removed because in practice it became a nuisance. Respectful users with
            // shared access to a feed can generally be trusted not to boot one another out in a combative manner.
            boolean overwrite = Boolean.parseBoolean(req.queryParams("overwrite"));
            if (userProfile != null && overwrite) {
                return invalidateAndCreateNewSession(
                    parsedReq,
                    "Previous session lock has been overwritten with new session.",
                    String.format("Previously active session %s has been overwritten with new session.", currentSession.sessionId)
                );
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
                parsedReq.sessionId
            );
            // Otherwise, the current session matches the session from which the delete request came. This indicates that
            // the user's editing session has been closed (by either exiting the editor or closing the browser tab).
            LOG.info("Closed session {} for feed {} successfully.", currentSession.sessionId, currentSession.feedId);
            sessionsForFeedIds.remove(parsedReq.getSessionKey());
            return formatSuccessJSON("Session has been closed successfully.", parsedReq.feedId, parsedReq.sessionId);
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

    private static String formatSuccessJSON(String message, String feedId, String sessionId) {
        JsonObject object = new JsonObject();
        object.addProperty("result", "OK");
        object.addProperty("message", message);
        object.addProperty("code", 200);
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
        public final String lockedItem;

        EditorSession (String feedId, String sessionId, Auth0UserProfile userProfile, String itemToLock) {
            this.feedId = feedId;
            this.sessionId = sessionId;
            this.userId = userProfile != null ? userProfile.getUser_id() : "no_user_id";
            this.userEmail = userProfile != null ? userProfile.getEmail() : "no_user_email";
            this.lastCheckIn = System.currentTimeMillis();
            this.lockedItem = itemToLock;
        }

        public long secondsSinceLastCheckIn() {
            return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - lastCheckIn);
        }

        public long minutesUntilExpiration() {
            return TimeUnit.SECONDS.toMinutes(SESSION_LENGTH_IN_SECONDS - secondsSinceLastCheckIn());
        }
    }

    /**
     * Holds useful data from an editor lock request.
     */
    public static class ParsedRequest {
        public final Request request;
        public final Auth0UserProfile userProfile;
        public final String feedId;
        public final String itemToLock;
        public final String sessionId;

        public ParsedRequest(Request req) {
            this.request = req;
            this.userProfile = req.attribute("user");
            this.feedId = req.queryParams("feedId");
            this.itemToLock = req.queryParamOrDefault("item", "");
            this.sessionId = req.params("id");
        }

        /**
         * Returns a composite key made of:
         * - a feed id that the lock affects,
         * - an id of the item being locked by that user in a session.
         */
        public String getSessionKey() {
            return String.format("%s-%s", feedId, itemToLock);
        }
    }
}
