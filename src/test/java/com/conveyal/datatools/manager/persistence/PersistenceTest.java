package com.conveyal.datatools.manager.persistence;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.UnitTest;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Created by landon on 9/6/17.
 */
public class PersistenceTest extends UnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(PersistenceTest.class);
    private static FeedSource feedSource;
    private static Project project;

    @BeforeAll
    public static void setUp() throws Exception {
        DatatoolsTest.setUp();
        LOG.info("{} setup", PersistenceTest.class.getSimpleName());

        Persistence.initialize();
    }

    @AfterAll
    public static void tearDown() {
        if (feedSource != null) Persistence.feedSources.removeById(feedSource.id);
        if (project != null) Persistence.projects.removeById(project.id);
    }

    @Test
    public void createFeedSource() {
        feedSource = new FeedSource("test feed source");
        String id = feedSource.id;
        Persistence.feedSources.create(feedSource);
        FeedSource feedSourceFromDB = Persistence.feedSources.getById(id);
        assertEquals(feedSource.fetchFrequency, feedSourceFromDB.fetchFrequency);
        assertEquals(
            id,
            feedSourceFromDB.id,
            "Found FeedSource ID should equal inserted ID.");
    }

//    @Test
//    public void createOrganization() {
//        Organization organization = new Organization();
//        organization.subscriptionBeginDate = new Date();
//        organization.subscriptionEndDate = new Date();
//        Persistence.organizations.create(organization);
//        String retrievedId = Persistence.organizations.getById(organization.id).id;
//        assertEquals("Found FeedSource ID should equal inserted ID.", retrievedId, organization.id);
//    }

//    @Test
//    public void doubleInsertFeedSource() {
//        FeedSource feedSource = new FeedSource("test feed source");
//        String id = feedSource.id;
//        Persistence.feedSources.create(feedSource);
//        Persistence.feedSources.create(feedSource);
//        String retrievedId = Persistence.feedSources.getById(id).id;
//        assertEquals("Found FeedSource ID should equal inserted ID.", retrievedId, id);
//    }
//
    @Test
    public void createProject() {
        project = new Project();
        String id = project.id;
        Persistence.projects.create(project);
        String retrievedId = Persistence.projects.getById(id).id;
        assertEquals(
            id,
            retrievedId,
            "Found Project ID should equal inserted ID.");
    }
//
//    @Test
//    public void createDeployment() {
//        Deployment deployment = new Deployment();
//        String id = deployment.id;
//        Persistence.deployments.insertOne(deployment);
//        String retrievedId = Persistence.deployments.find(eq(id)).first().id;
//        assertEquals("Found Deployment ID should equal inserted ID.", retrievedId, id);
//    }
//
//    @Test
//    public void createNote() {
//        Note note = new Note();
//        String id = note.id;
//        Persistence.notes.insertOne(note);
//        String retrievedId = Persistence.notes.find(eq(id)).first().id;
//        assertEquals("Found Note ID should equal inserted ID.", retrievedId, id);
//    }
//
//    @Test
//    public void createOrganization() {
//        Organization organization = new Organization();
//        String id = organization.id;
//        Persistence.organizations.insertOne(organization);
//        String retrievedId = Persistence.organizations.find(eq(id)).first().id;
//        assertEquals("Found Organization ID should equal inserted ID.", retrievedId, id);
//    }
//
//    @Test
//    public void createFeedVersion() {
//        FeedVersion feedVersion = new FeedVersion();
//        String id = feedVersion.id;
//        Persistence.feedVersions.insertOne(feedVersion);
//        String retrievedId = Persistence.feedVersions.find(eq(id)).first().id;
//        assertEquals("Found FeedVersion ID should equal inserted ID.", retrievedId, id);
//    }
//
//    @Test
//    public void deleteFeedSource() {
//        FeedSource feedSource = new FeedSource("test feed source");
//        String id = feedSource.id;
//        Persistence.feedSources.insertOne(feedSource);
//        DeleteResult deleteResult = Persistence.feedSources.deleteOne(eq(id));
//        assertEquals("Found FeedSource ID should equal inserted ID.", deleteResult.getDeletedCount(), 1);
//    }
//
//    @Test
//    public void deleteProject() {
//        Project project = new Project();
//        String id = project.id;
//        Persistence.projects.insertOne(project);
//        DeleteResult deleteResult = Persistence.projects.deleteOne(eq(id));
//        assertEquals("Found Project ID should equal inserted ID.", deleteResult.getDeletedCount(), 1);
//    }
//
//    @Test
//    public void deleteDeployment() {
//        Deployment deployment = new Deployment();
//        String id = deployment.id;
//        Persistence.deployments.insertOne(deployment);
//        DeleteResult deleteResult = Persistence.deployments.deleteOne(eq(id));
//        assertEquals("Found Deployment ID should equal inserted ID.", deleteResult.getDeletedCount(), 1);
//    }
//
//    @Test
//    public void deleteNote() {
//        Note note = new Note();
//        String id = note.id;
//        Persistence.notes.insertOne(note);
//        DeleteResult deleteResult = Persistence.notes.deleteOne(eq(id));
//        assertEquals("Found Note ID should equal inserted ID.", deleteResult.getDeletedCount(), 1);
//    }
//
//    @Test
//    public void deleteOrganization() {
//        Organization organization = new Organization();
//        String id = organization.id;
//        Persistence.organizations.insertOne(organization);
//        DeleteResult deleteResult = Persistence.organizations.deleteOne(eq(id));
//        assertEquals("Found Organization ID should equal inserted ID.", deleteResult.getDeletedCount(), 1);
//    }
//
//    @Test
//    public void deleteFeedVersion() {
//        FeedVersion feedVersion = new FeedVersion();
//        String id = feedVersion.id;
//        Persistence.feedVersions.insertOne(feedVersion);
//        DeleteResult deleteResult = Persistence.feedVersions.deleteOne(eq(id));
//        assertEquals("Found FeedVersion ID should equal inserted ID.", deleteResult.getDeletedCount(), 1);
//    }
//
//    @Test
//    public void updateFeedSource() {
//        FeedSource feedSource = new FeedSource("test feed source");
//        String id = feedSource.id;
//        String value = "test";
//        Persistence.feedSources.insertOne(feedSource);
//        UpdateResult updateResult = Persistence.feedSources.updateOne(eq(id), set("name", value));
//        FeedSource res = Persistence.feedSources.find(eq(id)).first();
//        assertEquals("Field 'name' should be updated.", res.name, value);
//        assertEquals("Found FeedSource ID should equal inserted ID.", updateResult.getModifiedCount(), 1);
//    }
//
//    @Test
//    public void updateProject() {
//        Project project = new Project();
//        String id = project.id;
//        String value = "test";
//        Persistence.projects.insertOne(project);
//        UpdateResult updateResult = Persistence.projects.updateOne(eq(id), set("name", value));
//        Project res = Persistence.projects.find(eq(id)).first();
//        assertEquals("Field 'name' should be updated.", res.name, value);
//        assertEquals("Found Project ID should equal inserted ID.", updateResult.getModifiedCount(), 1);
//    }
//
//    @Test
//    public void updateDeployment() {
//        Deployment deployment = new Deployment();
//        String id = deployment.id;
//        String value = "test";
//        Persistence.deployments.insertOne(deployment);
//        UpdateResult updateResult = Persistence.deployments.updateOne(eq(id), set("name", value));
//        Deployment res = Persistence.deployments.find(eq(id)).first();
//        assertEquals("Field 'name' should be updated.", res.name, value);
//        assertEquals("Found Deployment ID should equal inserted ID.", updateResult.getModifiedCount(), 1);
//    }
//
//    @Test
//    public void updateNote() {
//        Note note = new Note();
//        String id = note.id;
//        String value = "test";
//        Persistence.notes.insertOne(note);
//        UpdateResult updateResult = Persistence.notes.updateOne(eq(id), set("body", value));
//        Note res = Persistence.notes.find(eq(id)).first();
//        assertEquals("Field 'body' should be updated.", res.body, value);
//        assertEquals("Found Note ID should equal inserted ID.", updateResult.getModifiedCount(), 1);
//    }
//
//    @Test
//    public void updateOrganization() {
//        Organization organization = new Organization();
//        String id = organization.id;
//        String value = "test";
//        Persistence.organizations.insertOne(organization);
//        UpdateResult updateResult = Persistence.organizations.updateOne(eq(id), set("name", value));
//        Organization res = Persistence.organizations.find(eq(id)).first();
//        assertEquals("Field 'name' should be updated.", res.name, value);
//        assertEquals("Found Organization ID should equal inserted ID.", updateResult.getModifiedCount(), 1);
//    }
//
//    @Test
//    public void updateFeedVersion() {
//        FeedVersion feedVersion = new FeedVersion();
//        String id = feedVersion.id;
//        String value = "test";
//        Persistence.feedVersions.insertOne(feedVersion);
//        UpdateResult updateResult = Persistence.feedVersions.updateOne(eq(id), set("name", value));
//        FeedVersion res = Persistence.feedVersions.find(eq(id)).first();
//        assertEquals("Field 'name' should be updated.", res.name, value);
//        assertEquals("Found FeedVersion ID should equal inserted ID.", updateResult.getModifiedCount(), 1);
//    }
}
