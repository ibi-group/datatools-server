package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.NotifyUsersForSubscriptionJob;
import com.conveyal.datatools.manager.models.*;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.push;
import static spark.Spark.*;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Created by demory on 4/19/16.
 */
public class NoteController {

    private static JsonManager<Note> json =
            new JsonManager<Note>(Note.class, JsonViews.UserInterface.class);

    public static Collection<Note> getAllNotes (Request req, Response res) throws JsonProcessingException {
        Auth0UserProfile userProfile = req.attribute("user");
        if(userProfile == null) halt(401);

        String typeStr = req.queryParams("type");
        String objectId = req.queryParams("objectId");

        if (typeStr == null || objectId == null) {
            halt(400, "Please specify objectId and type");
        }

        Note.NoteType type = null;
        try {
            type = Note.NoteType.valueOf(typeStr);
        } catch (IllegalArgumentException e) {
            halt(400, "Please specify a valid type");
        }

        Model model = null;

        switch (type) {
            case FEED_SOURCE:
                model = Persistence.feedSources.getById(objectId);
                break;
            case FEED_VERSION:
                model = Persistence.feedVersions.getById(objectId);
                break;
            default:
                // this shouldn't ever happen, but Java requires that every case be covered somehow so model can't be used uninitialized
                halt(400, "Unsupported type for notes");
        }

        FeedSource s;

        if (model instanceof FeedSource) {
            s = (FeedSource) model;
        }
        else {
            s = ((FeedVersion) model).parentFeedSource();
        }
        String orgId = s.organizationId();
        // check if the user has permission
        if (userProfile.canAdministerProject(s.projectId, orgId) || userProfile.canViewFeed(orgId, s.projectId, s.id)) {
            return model.retrieveNotes();
        }
        else {
            halt(401);
        }

        return null;
    }

    public static Note createNote (Request req, Response res) throws IOException {
        Auth0UserProfile userProfile = req.attribute("user");
        if(userProfile == null) halt(401);

        String typeStr = req.queryParams("type");
        String objectId = req.queryParams("objectId");

        Note.NoteType type = null;
        try {
            type = Note.NoteType.valueOf(typeStr);
        } catch (IllegalArgumentException e) {
            halt(400, "Please specify a valid type");
        }

        Model objectWithNote = null;

        switch (type) {
            case FEED_SOURCE:
                objectWithNote = Persistence.feedSources.getById(objectId);
                break;
            case FEED_VERSION:
                objectWithNote = Persistence.feedVersions.getById(objectId);
                break;
            default:
                // this shouldn't ever happen, but Java requires that every case be covered somehow so model can't be used uninitialized
                halt(400, "Unsupported type for notes");
        }

        FeedSource feedSource;

        if (objectWithNote instanceof FeedSource) {
            feedSource = (FeedSource) objectWithNote;
        } else {
            feedSource = ((FeedVersion) objectWithNote).parentFeedSource();
        }
        String orgId = feedSource.organizationId();
        boolean allowedToCreate = userProfile.canAdministerProject(feedSource.projectId, orgId) ||
                userProfile.canViewFeed(orgId, feedSource.projectId, feedSource.id);
        if (allowedToCreate) {
            Note note = new Note();
            note.storeUser(userProfile);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(req.body());
            note.body = node.get("body").asText();

            note.userEmail = userProfile.getEmail();
            note.date = new Date();
            note.type = type;

            Persistence.notes.create(note);

            // TODO: figure out a cleaner way to handle this update
            if (objectWithNote instanceof FeedSource) {
                Persistence.feedSources.getMongoCollection().updateOne(eq(objectWithNote.id), push("noteIds", note.id));
            } else {
                Persistence.feedVersions.getMongoCollection().updateOne(eq(objectWithNote.id), push("noteIds", note.id));
            }

            // send notifications
            NotifyUsersForSubscriptionJob notifyFeedJob = new NotifyUsersForSubscriptionJob("feed-commented-on", feedSource.id, note.userEmail + " commented on " + feedSource.name + " at " + note.date.toString() + ":<blockquote>" + note.body + "</blockquote>");
            DataManager.lightExecutor.execute(notifyFeedJob);

            return note;
        }
        else {
            halt(401);
        }

        return null;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/note", NoteController::getAllNotes, json::write);
        post(apiPrefix + "secure/note", NoteController::createNote, json::write);
    }
}
