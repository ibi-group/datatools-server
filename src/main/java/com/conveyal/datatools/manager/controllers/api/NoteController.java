package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.NotifyUsersForSubscriptionJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.models.Model;
import com.conveyal.datatools.manager.models.Note;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import org.eclipse.jetty.http.HttpStatus;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;

import static com.conveyal.datatools.common.utils.SparkUtils.logMessageAndHalt;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.push;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Handlers for HTTP API requests that affect {@link Note} objects (comments attached to a {@link FeedSource} or
 * {@link FeedVersion}.
 */
public class NoteController {
    public static final String UNAUTHORIZED_MESSAGE = "User not authorized to perform this action";
    private static final JsonManager<Note> json = new JsonManager<>(Note.class, JsonViews.UserInterface.class);

    public static Collection<Note> getAllNotes (Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");
        // Note: Halt will handle early return if objectWithNote is null.
        Model objectWithNote = getObjectWithNote(req);
        FeedSource feedSource = getFeedSourceForModel(objectWithNote);
        // check if the user has permission to get notes for this feed source.
        checkPermissions(req, feedSource);
        // Return notes (only including admin only notes if project admin).
        return objectWithNote.retrieveNotes(userProfile.canAdministerProject(feedSource));
    }

    public static Note createNote (Request req, Response res) throws IOException {
        // Note: Halt will handle early return if userProfile is null.
        Auth0UserProfile userProfile = req.attribute("user");
        Model objectWithNote = getObjectWithNote(req);
        // Note: Halt will handle early return if objectWithNote is null.
        FeedSource feedSource = getFeedSourceForModel(objectWithNote);
        // Ensure that user has create note permission.
        checkPermissions(req, feedSource);
        Note note = new Note();
        note.storeUser(userProfile);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(req.body());
        note.body = node.get("body").asText();

        note.userEmail = userProfile.getEmail();
        note.date = new Date();
        note.type = getTypeFromRequest(req);
        note.adminOnly = node.get("adminOnly").asBoolean();
        if (note.adminOnly && !userProfile.canAdministerProject(feedSource)) {
            logMessageAndHalt(
                req,
                HttpStatus.UNAUTHORIZED_401,
                "User must be admin to create admin-only note."
            );
        }
        // Create note and update feed source or version with noteIds
        Persistence.notes.create(note);
        MongoCollection collection = objectWithNote instanceof FeedSource
            ? Persistence.feedSources.getMongoCollection()
            : Persistence.feedVersions.getMongoCollection();
        collection.updateOne(eq(objectWithNote.id), push("noteIds", note.id));
        String message = String.format(
                "%s commented on %s at %s:<blockquote>%s</blockquote>",
                note.userEmail,
                feedSource.name,
                note.date.toString(),
                note.body);
        // Send notifications to comment subscribers.
        // TODO: feed-commented-on has been merged into feed-updated subscription type. This should be clarified
        //  in the subject line/URL of the notification email.
        NotifyUsersForSubscriptionJob.createNotification(
            "feed-updated",
            feedSource.id,
            message
        );
        return note;
    }

    /**
     * Check that the requesting user is permitted to view/create a note for this feed source. Throws halt if not.
     */
    private static void checkPermissions(Request req, FeedSource feedSource) {
        Auth0UserProfile userProfile = req.attribute("user");
        boolean hasNotePermission = userProfile.canAdministerProject(feedSource) || userProfile.canViewFeed(feedSource);
        if (!hasNotePermission) {
            logMessageAndHalt(req, HttpStatus.UNAUTHORIZED_401, UNAUTHORIZED_MESSAGE);
        }
    }

    /**
     * Grab the {@link FeedSource} from the given object (which must be either a FeedSource itself or a
     * {@link FeedVersion}.
     */
    private static FeedSource getFeedSourceForModel(Model objectWithNote) {
        return objectWithNote instanceof FeedSource
            ? (FeedSource) objectWithNote
            : ((FeedVersion) objectWithNote).parentFeedSource();
    }

    /**
     * Get the {@link com.conveyal.datatools.manager.models.Note.NoteType} from the Spark HTTP request.
     */
    private static Note.NoteType getTypeFromRequest(Request req) {
        String typeStr = req.queryParams("type");
        Note.NoteType type = null;
        try {
            type = Note.NoteType.valueOf(typeStr);
        } catch (IllegalArgumentException e) {
            logMessageAndHalt(req, 400, "Please specify a valid type");
        }
        return type;
    }

    /**
     * Get generic {@link Model} object that the note is attached to (via objectId query param).
     */
    private static Model getObjectWithNote(Request req) {
        String objectId = req.queryParams("objectId");
        Note.NoteType type = getTypeFromRequest(req);
        switch (type) {
            case FEED_SOURCE:
                return Persistence.feedSources.getById(objectId);
            case FEED_VERSION:
                return Persistence.feedVersions.getById(objectId);
            default:
                // this shouldn't ever happen, but Java requires that every case be covered somehow so model can't
                // be used uninitialized
                logMessageAndHalt(req, 400, "Unsupported type for notes");
                return null;
        }
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/note", NoteController::getAllNotes, json::write);
        post(apiPrefix + "secure/note", NoteController::createNote, json::write);
    }
}
