package com.conveyal.datatools.manager.persistence;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Model;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Updates.pull;

/**
 * This provides some abstraction over the Mongo Java driver for storing a particular kind of POJO.
 *
 * When performing an update (in our case with findOneAndUpdate) the Document of updates
 * may contain extra fields beyond those in the Java model class, or values of a type that
 * do not match the Java model class. The update will nonetheless add these extra fields
 * and wrong-typed values to MongoDB, which is not shocking considering its schemaless
 * nature. Of course a retrieved Java object will not contain these extra values
 * because it simply doesn't have a field to hold the values. If a value of the wrong
 * type has been stored in the database, deserialization will just fail with
 * "org.bson.codecs.configuration.CodecConfigurationException: Failed to decode X."
 *
 * This means clients have the potential to stuff any amount of garbage in our MongoDB
 * and trigger deserialization errors during application execution unless we perform
 * type checking and clean the incoming documents. There is probably a configuration
 * option to force schema adherence, which would prevent long-term compatibility but
 * would give us more safety in the short term.
 *
 * PojoCodecImpl does not seem to have any hooks to throw errors when unexpected fields
 * are encountered (see else clause of
 * org.bson.codecs.pojo.PojoCodecImpl#decodePropertyModel). We could make our own
 * function to imitate the PropertyModel checking and fail early when unexpected fields
 * are present in a document.
 */
public class TypedPersistence<T extends Model> {

    private static final Logger LOG = LoggerFactory.getLogger(TypedPersistence.class);

    private MongoCollection<T> mongoCollection;
    private Constructor<T> noArgConstructor;
    private String collectionName;
    private final FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions();

    /**
     * Maps a persistence class to a Mongo collection.
     */
    public TypedPersistence(MongoDatabase mongoDatabase, Class<T> clazz, String collectionName) {
        mongoCollection = mongoDatabase.getCollection(collectionName, clazz);
        this.collectionName = collectionName;
        try {
            noArgConstructor = clazz.getConstructor(new Class<?>[0]);
        } catch (NoSuchMethodException ex) {
            throw new RuntimeException("Could not get no-arg constructor for class " + clazz.getName(), ex);
        }
        // set options for findOneAndUpdate (return document should match document after update, not before)
        findOneAndUpdateOptions.returnDocument(ReturnDocument.AFTER);

        // TODO: can we merge update and create into createOrUpdate function using upsert option?
//        findOneAndUpdateOptions.upsert(true);
    }

    /**
     * Shorthand for above constructor using the class name as the collection name.
     */
    public TypedPersistence(MongoDatabase mongoDatabase, Class<T> clazz) {
        this(mongoDatabase, clazz, clazz.getSimpleName());
    }

    /**
     * Count the documents matching the provided filter.
     */
    public long count (Bson filter) {
        return mongoCollection.countDocuments(filter);
    }

    public T create (String updateJson) {
        T item = null;
        try {
            // Keeping our own reference to the constructor here is a little shady.
            // FIXME: We should try to use some Mongo codec method for this, e.g. inserting an empty document.
            item = noArgConstructor.newInstance();
        } catch (Exception ex) {
            throw new RuntimeException("Could not use no-arg constructor to instantiate class.", ex);
        }
        mongoCollection.insertOne(item);
        T updatedItem = update(item.id, updateJson);
        return updatedItem;
    }

    /**
     * TODO maybe merge this with the other create implementation above, passing in the base object and the updates.
     */
    public void create (T newObject) {
        // What happens if an object already exists with the same ID?
        mongoCollection.insertOne(newObject);
    }

    public void replace (String id, T replaceObject) {
        mongoCollection.replaceOne(eq(id), replaceObject);
    }

    /**
     * Primary method to update Mongo object with provided document. This sets the lastUpdated field to the current time.
     */
    public T update (String id, Document updateDocument) {
        // Set last updated.
        updateDocument.put("lastUpdated", new Date());
        return mongoCollection.findOneAndUpdate(eq(id), new Document("$set", updateDocument), findOneAndUpdateOptions);
    }

    /**
     * Update Mongo object by ID with the provided JSON string.
     */
    public T update (String id, String updateJson) {
        return update(id, Document.parse(updateJson));
    }

    /**
     * Update the field with the provided value for the Mongo object referenced by ID.
     */
    public T updateField (String id, String fieldName, Object value) {
        return update(id, new Document(fieldName, value));
    }

    public T updateUser (String id, Auth0UserProfile profile) {
        String updateJson = String.format("{userId: %s, userEmail: %s}", profile.getUser_id(), profile.getEmail());
        Document updateDocument = Document.parse(updateJson);
        return mongoCollection.findOneAndUpdate(eq(id), new Document("$set", updateDocument), findOneAndUpdateOptions);
    }

    public T getById (String id) {
        return mongoCollection.find(eq(id)).first();
    }

    /**
     * Get first matching object populating only the included fields.
     */
    public T getByIdLimitedFields (String id, String... includedFields) {
        return mongoCollection
            .find(eq(id))
            .projection(fields(include(includedFields)))
            .first();
    }

    /**
     * This is not memory efficient.
     * TODO: Always use iterators / streams, always perform selection of subsets on the Mongo server side ("where clause").
     */
    public List<T> getAll () {
        return mongoCollection.find().into(new ArrayList<>());
    }

    /**
     * Gets all objects with one of the IDs passed
     * @param ids   List of IDs to return objects with
     * @return      Objects in the collection with given IDs
     */
    public List<T> getByIds (List<String> ids) {
        return mongoCollection.find(in("_id", ids)).into(new ArrayList<>());
    }

    /**
     * Get all objects satisfying the supplied Mongo filter.
     * This ties our persistence directly to Mongo for now but is expedient.
     * We should really have a bit more abstraction here.
     */
    public List<T> getFiltered (Bson filter) {
        return getFiltered(filter, null);
    }

    /**
     * Get all objects satisfying the supplied Mongo filter and sort by.
     */
    public List<T> getFiltered (Bson filter, Bson sortBy) {
        return (sortBy != null)
        ? mongoCollection.find(filter).sort(sortBy).into(new ArrayList<>())
        : mongoCollection.find(filter).into(new ArrayList<>());
    }

    /**
     * Get all objects populating only the included fields satisfying the supplied Mongo filter and sort by.
     */
    public List<T> getFilteredLimitedFields(Bson filter, Bson sortBy, String... includedFields) {
        return mongoCollection
            .find(filter)
            .projection(fields(include(includedFields)))
            .sort(sortBy)
            .into(new ArrayList<>());
    }

    /**
     * Expose the internal MongoCollection to the caller.
     * This ties our persistence directly to Mongo for now but is expedient.
     * We will write all the queries we need in the calling methods, then make an abstraction here on TypedPersistence
     * once we see everything we need to support.
     */
    public MongoCollection<T> getMongoCollection () {
        return this.mongoCollection;
    }

    /**
     * Get all objects satisfying the supplied Mongo filter.
     * This ties our persistence directly to Mongo for now but is expedient.
     * We should really have a bit more abstraction here.
     */
    public T getOneFiltered (Bson filter, Bson sortBy) {
        if (sortBy != null)
            return mongoCollection.find(filter).sort(sortBy).first();
        else
            return mongoCollection.find(filter).first();
    }

    /** Convenience wrapper for #getOneFiltered that supplies null for sortBy arg. */
    public T getOneFiltered (Bson filter) {
        return getOneFiltered(filter, null);
    }

    public boolean removeById (String id) {
        DeleteResult result = mongoCollection.deleteOne(eq(id));
        if (result.getDeletedCount() == 1) {
            LOG.info("Deleted object id={} type={}", id, collectionName);
            return true;
        } else if (result.getDeletedCount() > 1) {
            LOG.error("Deleted more than one {} for ID {}",collectionName, id);
        } else {
            LOG.error("Could not delete {}: {}", collectionName, id);
        }
        return false;
    }

    /**
     * Generic method to remove a specific note from the {@link Model#noteIds} list.
     */
    public UpdateResult removeNoteFromCollection(String noteId) {
        // Finds all objects that contain the note ID and "pulls" (removes) the ID from the list.
        return updateMany(in("noteIds", noteId), pull("noteIds", noteId));
    }

    /**
     * Apply update filter to documents that match query.
     */
    public UpdateResult updateMany(Bson query, Bson update) {
        return mongoCollection.updateMany(query, update);
    }

    public boolean removeFiltered (Bson filter) {
        DeleteResult result = mongoCollection.deleteMany(filter);
        long count = result.getDeletedCount();
        if (count >= 1) {
            LOG.info("Deleted {} objects of type {}", count, collectionName);
            return true;
        } else {
            LOG.warn("No objects to delete for filter");
        }
        return false;
    }

}
