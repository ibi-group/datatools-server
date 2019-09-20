package com.conveyal.datatools.manager.persistence;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.codec.IntArrayCodec;
import com.conveyal.datatools.manager.codec.LocalDateCodec;
import com.conveyal.datatools.manager.codec.URLCodec;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedDownloadToken;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Note;
import com.conveyal.datatools.manager.models.Organization;
import com.conveyal.datatools.manager.models.OtpServer;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.models.Snapshot;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * Groups together a bunch of TypedPersistence abstractions around MongoDB Collections.
 */
public class Persistence {

    private static final Logger LOG = LoggerFactory.getLogger(Persistence.class);
    private static final String MONGO_URI = "MONGO_URI";
    private static final String MONGO_DB_NAME = "MONGO_DB_NAME";

    private static MongoClient mongo;
    private static MongoDatabase mongoDatabase;
    private static CodecRegistry pojoCodecRegistry;

    // One abstracted Mongo collection for each class of persisted objects
    public static TypedPersistence<FeedSource> feedSources;
    public static TypedPersistence<Deployment> deployments;
    public static TypedPersistence<Project> projects;
    public static TypedPersistence<FeedVersion> feedVersions;
    public static TypedPersistence<Note> notes;
    public static TypedPersistence<Organization> organizations;
    public static TypedPersistence<ExternalFeedSourceProperty> externalFeedSourceProperties;
    public static TypedPersistence<OtpServer> servers;
    public static TypedPersistence<Snapshot> snapshots;
    public static TypedPersistence<FeedDownloadToken> tokens;

    public static void initialize () {

        PojoCodecProvider pojoCodecProvider = PojoCodecProvider.builder()
                .register("com.conveyal.datatools.manager.jobs")
                .register("com.conveyal.datatools.manager.models")
                .register("com.conveyal.gtfs.loader")
                .register("com.conveyal.gtfs.validator")
                .automatic(true)
                .build();

        // Register our custom codecs which cannot be properly auto-built by reflection
        CodecRegistry customRegistry = CodecRegistries.fromCodecs(
                new IntArrayCodec(),
                new URLCodec(),
                new LocalDateCodec());

        pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                customRegistry,
                fromProviders(pojoCodecProvider));

        MongoClientOptions.Builder builder = MongoClientOptions.builder()
//                .sslEnabled(true)
                .codecRegistry(pojoCodecRegistry);

        if (DataManager.hasConfigProperty(MONGO_URI)) {
            mongo = new MongoClient(new MongoClientURI(DataManager.getConfigPropertyAsText(MONGO_URI), builder));
            LOG.info("Connecting to remote MongoDB instance");
        } else {
            LOG.info("Connecting to local MongoDB instance");
            mongo = new MongoClient("localhost", builder.build());
        }

        mongoDatabase = mongo.getDatabase(DataManager.getConfigPropertyAsText(MONGO_DB_NAME));

        feedSources = new TypedPersistence(mongoDatabase, FeedSource.class);
        projects = new TypedPersistence(mongoDatabase, Project.class);
        feedVersions = new TypedPersistence(mongoDatabase, FeedVersion.class);
        deployments = new TypedPersistence(mongoDatabase, Deployment.class);
        notes = new TypedPersistence(mongoDatabase, Note.class);
        organizations = new TypedPersistence(mongoDatabase, Organization.class);
        externalFeedSourceProperties = new TypedPersistence(mongoDatabase, ExternalFeedSourceProperty.class);
        servers = new TypedPersistence(mongoDatabase, OtpServer.class);
        snapshots = new TypedPersistence(mongoDatabase, Snapshot.class);
        tokens = new TypedPersistence(mongoDatabase, FeedDownloadToken.class);

        // TODO: Set up indexes on feed versions by feedSourceId, version #? deployments, feedSources by projectId.
//        deployments.getMongoCollection().createIndex(Indexes.descending("projectId"));
//        feedSources.getMongoCollection().createIndex(Indexes.descending("projectId"));
//        feedVersions.getMongoCollection().createIndex(Indexes.descending("feedSourceId", "version"));
//        snapshots.getMongoCollection().createIndex(Indexes.descending("feedSourceId", "version"));
    }
    
}
