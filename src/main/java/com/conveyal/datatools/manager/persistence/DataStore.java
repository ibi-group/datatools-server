package com.conveyal.datatools.manager.persistence;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.conveyal.datatools.manager.DataManager;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.Bind;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Fun.Function2;
import org.mapdb.Pump;
import org.mapdb.Fun.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStore<T> {
    private static final Logger LOG = LoggerFactory.getLogger(DataStore.class);

    DB db;
    BTreeMap<String,T> map;

    public DataStore(String dataFile) {
        this(new File(DataManager.getConfigPropertyAsText("application.data.mapdb")), dataFile);
    }

    public DataStore(File directory, String dataFile) {

        if(!directory.exists())
            directory.mkdirs();

        try {
            LOG.info(String.join("/", directory.getCanonicalPath(), dataFile));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        db = DBMaker.newFileDB(new File(directory, dataFile + ".db"))
                .closeOnJvmShutdown()
                .make();

        DB.BTreeMapMaker maker = db.createTreeMap(dataFile);
        maker.valueSerializer(new ClassLoaderSerializer());
        map = maker.makeOrGet();
    }

    public DataStore(File directory, String dataFile, List<Fun.Tuple2<String,T>>inputData) {

        if(!directory.exists())
            directory.mkdirs();

        try {
            LOG.info(String.join("/", directory.getCanonicalPath(), dataFile));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        db = DBMaker.newFileDB(new File(directory, dataFile + ".db"))
                .transactionDisable()
                .closeOnJvmShutdown()
                .make();

        Comparator<Tuple2<String, T>> comparator = (o1, o2) -> o1.a.compareTo(o2.a);

        // need to reverse sort list
        Iterator<Fun.Tuple2<String,T>> iter = Pump.sort(inputData.iterator(),
                true, 100000,
                Collections.reverseOrder(comparator), //reverse  order comparator
                db.getDefaultSerializer()
        );


        BTreeKeySerializer<String> keySerializer = BTreeKeySerializer.STRING;

        map = db.createTreeMap(dataFile)
                .pumpSource(iter)
                .pumpPresort(100000)
                .keySerializer(keySerializer)
                .make();



        // close/flush db
        db.close();

        // re-connect with transactions enabled
        db = DBMaker.newFileDB(new File(directory, dataFile + ".db"))
                .closeOnJvmShutdown()
                .make();

        map = db.getTreeMap(dataFile);
    }

    public void save(String id, T obj) {
        map.put(id, obj);
        db.commit();
    }

    public void saveWithoutCommit(String id, T obj) {
        map.put(id, obj);
    }

    public void commit() {
        db.commit();
    }

    public void delete(String id) {
        map.remove(id);
        db.commit();
    }

    public T getById(String id) {
        return map.get(id);
    }

    /**
     * Does an object with this ID exist in this data store?
     * @param id
     * @return boolean indicating result
     */
    public boolean hasId(String id) {
        return map.containsKey(id);
    }

    public Collection<T> getAll() {
        return map.values();
    }

    public Integer size() {
        return map.keySet().size();
    }

    /** Create a secondary (unique) key */
    public <K2> void secondaryKey (String name, Function2<K2, String, T> fun) {
        Map<K2, String> index = db.getTreeMap(name);
        Bind.secondaryKey(map, index, fun);
    }

    /** search using a secondary unique key */
    public <K2> T find(String name, K2 value) {
        Map<K2, String> index = db.getTreeMap(name);

        String id = index.get(value);

        if (id == null)
            return null;

        return map.get(id);
    }

    /** find the value with largest key less than or equal to key */
    public <K2> T findFloor (String name, K2 floor) {
        BTreeMap<K2, String> index = db.getTreeMap(name);

        Entry<K2, String> key = index.floorEntry(floor);

        if (key == null || key.getValue() == null)
            return null;

        return map.get(key.getValue());
    }
}