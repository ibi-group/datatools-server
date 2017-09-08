package com.conveyal.datatools.manager.persistence;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.codec.LocalDateCodec;
import com.conveyal.datatools.manager.codec.Rectangle2DCodec;
import com.conveyal.datatools.manager.codec.URLCodec;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Note;
import com.conveyal.datatools.manager.models.Organization;
import com.conveyal.datatools.manager.models.Project;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * Created by landon on 9/5/17.
 */
public class Persistence {
    private static final Logger LOG = LoggerFactory.getLogger(Persistence.class);

    private static MongoClient mongo;
    private static MongoDatabase mongoDatabase;

    // Mongo collections for all serialized collections
    private static MongoCollection<TestThing> testThingMongoCollection;
    public static MongoCollection<FeedSource> feedSources;
    public static MongoCollection<Deployment> deployments;
    public static MongoCollection<Project> projects;
    public static MongoCollection<FeedVersion> feedVersions;
    public static MongoCollection<Note> notes;
    public static MongoCollection<Organization> organizations;
    public static MongoCollection<ExternalFeedSourceProperty> externalFeedSourceProperties;

    public static void initialize () {
        PojoCodecProvider pojoCodecProvider = PojoCodecProvider.builder()
                .register("com.conveyal.datatools.manager.models")
                .automatic(true)
                .build();

        // Add custom codecs
        CodecRegistry customRegistry = CodecRegistries.fromCodecs(
                new URLCodec(),
                new Rectangle2DCodec(),
                new LocalDateCodec());
        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                customRegistry,
                fromProviders(pojoCodecProvider));

        MongoClientOptions.Builder builder = MongoClientOptions.builder()
//                .sslEnabled(true)
                .codecRegistry(pojoCodecRegistry);

        if (DataManager.hasConfigProperty("mongo_uri")) {
            mongo = new MongoClient(new MongoClientURI(DataManager.getConfigPropertyAsText("mongo_uri"), builder));
            LOG.info("Connecting to remote MongoDB instance");
        }
        else {
            LOG.info("Connecting to local MongoDB instance");
            mongo = new MongoClient("localhost", builder.build());
        }

        mongoDatabase = mongo.getDatabase("nest");

        testThingMongoCollection = mongoDatabase.getCollection(TestThing.class.getSimpleName(), TestThing.class);
        feedSources = mongoDatabase.getCollection(FeedSource.class.getSimpleName(), FeedSource.class);
        projects = mongoDatabase.getCollection(Project.class.getSimpleName(), Project.class);
        feedVersions = mongoDatabase.getCollection(FeedVersion.class.getSimpleName(), FeedVersion.class);
        deployments = mongoDatabase.getCollection(Deployment.class.getSimpleName(), Deployment.class);
        notes = mongoDatabase.getCollection(Note.class.getSimpleName(), Note.class);
        organizations = mongoDatabase.getCollection(Organization.class.getSimpleName(), Organization.class);
        externalFeedSourceProperties = mongoDatabase.getCollection(ExternalFeedSourceProperty.class.getSimpleName(), ExternalFeedSourceProperty.class);
    }

//    public T getById (String id) {
//        return
//    }

    public static Project createProject(Project project, Document updateDocument) {
        // TODO!!!! fix all of this, use updateProject
//        return Persistence.projects.findOneAndUpdate(eq(), new Document("$set", updateDocument));
        return project;
    }

    public static Project updateProject(String id, Document updateDocument) {
        return Persistence.projects.findOneAndUpdate(eq(id), new Document("$set", updateDocument));
    }

    public static Project getProjectById(String id) {
        return projects.find(eq(id)).first();
    }

    public static List<Project> getProjects() {
        return projects.find().into(new ArrayList<>());
    }


    public static boolean removeProject(String id) {
        DeleteResult result = projects.deleteOne(eq(id));
        if (result.getDeletedCount() == 1) {
            LOG.info("Deleted project: {}", id);
            return true;
        } else {
            LOG.warn("Could not delete project: {}", id);
            return false;
        }
    }

    public static FeedSource getFeedSourceById(String id) {
        return feedSources.find(eq(id)).first();
    }

    public static List<FeedSource> getFeedSources() {
        return feedSources.find().into(new ArrayList<>());
    }

    public static boolean removeFeedSource(String id) {
        DeleteResult result = feedSources.deleteOne(eq(id));
        if (result.getDeletedCount() == 1) {
            LOG.info("Deleted feedSource: {}", id);
            return true;
        } else {
            LOG.warn("Could not delete feedSource: {}", id);
            return false;
        }
    }

    public static TestThing createThing(TestThing testThing) {
        testThingMongoCollection.insertOne(testThing);
        return retrieveThing(testThing.id);
    }

    public static TestThing retrieveThing(String id) {
        return testThingMongoCollection.find(eq(id)).first();
    }

    public static TestThing updateThing(TestThing testThing, String field, Object property) {
        testThingMongoCollection.updateOne(eq(testThing.id), set(field, property));
        return retrieveThing(testThing.id);
    }

    public static boolean removeThing(String id) {
        DeleteResult result = testThingMongoCollection.deleteOne(eq(id));
        if (result.getDeletedCount() == 1) {
//            LOG.info("Deleted thing: {}", id);
            return true;
        } else {
            LOG.warn("Could not delete thing: {}", id);
            return false;
        }
    }
}
