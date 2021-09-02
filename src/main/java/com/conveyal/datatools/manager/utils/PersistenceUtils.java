package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.manager.models.Label;
import com.conveyal.datatools.manager.models.Note;
import org.bson.conversions.Bson;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

/**
 * Contains utilities specifically for operations related to the MongoDB database.
 */
public class PersistenceUtils {

    /**
     * @return admin filter (relevant for {@link Label} and {@link Note} classes) ANDed to the input filter or just the
     *  original filter.
     */
    public static Bson applyAdminFilter(Bson filter, boolean isAdmin) {
        return isAdmin ? filter : and(filter, eq("adminOnly", false));
    }
}
