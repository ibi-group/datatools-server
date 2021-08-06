package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.TestUtils;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Model;
import com.conveyal.datatools.manager.models.Note;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.HttpUtils;
import com.conveyal.datatools.manager.utils.SimpleHttpResponse;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.HttpResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static org.eclipse.jetty.http.HttpStatus.UNAUTHORIZED_401;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class NoteControllerTest {
    private static Project project = null;
    private static FeedSource feedSource = null;
    private static FeedVersion feedVersion = null;
    private static Auth0UserProfile viewOnlyUser;

    @BeforeAll
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        Auth0Connection.setAuthDisabled(true);
        project = new Project();
        project.name = "Project-Notes-Test";
        Persistence.projects.create(project);
        feedSource = new FeedSource("Feed with Notes", project.id, FeedRetrievalMethod.MANUALLY_UPLOADED);
        Persistence.feedSources.create(feedSource);
        feedVersion = TestUtils.createFeedVersionFromGtfsZip(feedSource, "bart_old.zip");
        viewOnlyUser = Auth0UserProfile.createTestViewOnlyUser(project.id);
    }

    @AfterAll
    public static void tearDown() {
        Auth0Connection.setAuthDisabled(Auth0Connection.getDefaultAuthDisabled());
        project.delete();
    }

    @AfterEach
    public void afterEach() {
        getNotes(feedSource, true).forEach(Note::delete);
        getNotes(feedVersion, true).forEach(Note::delete);
        // Reset test user to default.
        Auth0Connection.setTestUser(Auth0Connection.getDefaultTestUser());
    }

    @Test
    public void deletedNoteRemovesReference() throws IOException {
        final String body = "This feed source is amazing.";
        // Create feed source note
        Note feedNote = createNoteRequestWithSuccessAssertion(createNote(body, false), feedSource);
        // Create feed version note
        createNoteRequestWithSuccessAssertion(createNote(body, false), feedVersion);
        assertEquals(1, getNotes(feedSource, true).size());
        assertEquals(1, getNotes(feedVersion, true).size());
        // Delete note and verify that FeedSource#noteIds is updated.
        feedNote.delete();
        assertEquals(0, getNotes(feedSource, true).size());
        // Ensure feed version note is untouched
        assertEquals(1, getNotes(feedVersion, true).size());
    }

    @Test
    public void canCreateNoteForFeedSource() throws IOException {
        final String body = "This feed source is amazing.";
        Note note = createNote(body, false);
        Note createdNote = createNoteRequestWithSuccessAssertion(note, feedSource);
        List<Note> notes = getNotes(feedSource, true);
        assertEquals(1, notes.size());
        for (Note feedNote : notes) {
            if (feedNote.id.equals(createdNote.id)) {
                assertEquals(body, feedNote.body);
            }
        }
    }

    @Test
    public void canCreateNoteForFeedVersion() throws IOException {
        final String body = "This feed version is amazing.";
        Note note = createNote(body, false);
        Note createdNote = createNoteRequestWithSuccessAssertion(note, feedVersion);
        List<Note> notes = getNotes(feedVersion, true);
        assertEquals(1, notes.size());
        for (Note versionNote : notes) {
            if (versionNote.id.equals(createdNote.id)) {
                assertEquals(body, versionNote.body);
            }
        }
    }

    @Test
    public void willPreventNonAdminFromCreatingAdminOnlyLabel() {
        // Override test user with view-only user.
        Auth0Connection.setTestUser(viewOnlyUser);
        // Attempt to create admin only note.
        final String body = "I'm only a standard user, but am trying to make an admin note.";
        Note note = createNote(body, true);
        SimpleHttpResponse createNoteResponse = createNoteRequest(note, feedSource);
        // Verify that no note was created.
        assertEquals(UNAUTHORIZED_401, createNoteResponse.status);
        List<Note> notes = getNotes(feedSource, true);
        assertEquals(0, notes.size());
    }

    /**
     * Verifies that we can create an admin note and filter it out for non-project admins.
     */
    @Test
    public void willFilterOutAdminNotesForNonAdmins() throws IOException {
        final String secureBody = "This is a secret note!";
        // Create admin note
        Note adminNote = createNote(secureBody, true);
        createNoteRequestWithSuccessAssertion(adminNote, feedSource);
        // Create normal note
        final String body = "This feed source is really great.";
        Note note = createNote(body, false);
        Note createdNote = createNoteRequestWithSuccessAssertion(note, feedSource);
        // Verify only one non-admin note is visible
        List<Note> filteredNotes = getNotes(feedSource, false);
        assertEquals(1, filteredNotes.size());
        for (Note versionNote : filteredNotes) {
            if (versionNote.id.equals(createdNote.id)) {
                assertEquals(body, versionNote.body);
            }
        }
        // Verify that both notes are visible to an admin
        List<Note> allNotes = getNotes(feedSource, true);
        assertEquals(2, allNotes.size());
        // Verify that a non-admin API request to get the feed source shows the correct/filtered note count.
        Auth0Connection.setTestUser(viewOnlyUser);
        String url = String.format("/api/manager/secure/feedsource/%s", feedSource.id);
        SimpleHttpResponse getFeedSourceResponse = TestUtils.makeRequest(url, null, HttpUtils.REQUEST_METHOD.GET);
        JsonNode feedSourceJson = JsonUtil.getJsonNodeFromResponse(getFeedSourceResponse);
        assertEquals(1, feedSourceJson.get("noteCount").asInt());
        // Fetch notes as non-admin and ensure the correct number.
        List<Note> notes = getNotesRequest(feedSource);
        assertEquals(1, notes.size());
    }

    /**
     * Convenience method for getting a fresh feed source/version from the DB and fetching its notes.
     */
    private List<Note> getNotes(Model model, boolean includeAdminNotes) {
        if (model instanceof FeedSource) {
            FeedSource feedSource = Persistence.feedSources.getById(model.id);
            return feedSource.retrieveNotes(includeAdminNotes);
        } else {
            FeedVersion version = Persistence.feedVersions.getById(model.id);
            return version.retrieveNotes(includeAdminNotes);
        }
    }

    private Note createNote(String body, boolean adminOnly) {
        Note note = new Note();
        note.body = body;
        note.adminOnly = adminOnly;
        return note;
    }

    private SimpleHttpResponse createNoteRequest(Note note, Model model) {
        String type = model instanceof FeedSource ? "FEED_SOURCE" : "FEED_VERSION";
        return TestUtils.makeRequest(
            "/api/manager/secure/note" + String.format("?type=%s&objectId=%s", type,  model.id),
            JsonUtil.toJson(note),
            HttpUtils.REQUEST_METHOD.POST
        );
    }

    private List<Note> getNotesRequest(Model model) throws IOException {
        String type = model instanceof FeedSource ? "FEED_SOURCE" : "FEED_VERSION";
        SimpleHttpResponse getNotesResponse = TestUtils.makeRequest(
            "/api/manager/secure/note" + String.format("?type=%s&objectId=%s", type, model.id),
            null,
            HttpUtils.REQUEST_METHOD.GET
        );
        return JsonUtil.getPOJOFromJSONAsList(JsonUtil.getJsonNodeFromResponse(getNotesResponse), Note.class);
    }

    private Note createNoteRequestWithSuccessAssertion(Note note, Model model) throws IOException {
        String type = model instanceof FeedSource ? "FEED_SOURCE" : "FEED_VERSION";
        SimpleHttpResponse noteResponse = TestUtils.makeRequest(
            "/api/manager/secure/note" + String.format("?type=%s&objectId=%s", type, model.id),
            JsonUtil.toJson(note),
            HttpUtils.REQUEST_METHOD.POST
        );
        assertEquals(OK_200, noteResponse.status);
        return JsonUtil.objectMapper.readValue(noteResponse.body, Note.class);
    }
}
