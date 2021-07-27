package com.conveyal.datatools.manager.serializers;

import com.conveyal.datatools.manager.models.Label;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * This serializer is used to convert a list of Strings representing label IDs into full Label objects
 * List<String> is serialized into List<Label>.
 */
public class LabelSerializer extends StdSerializer<ArrayList<String>> {

    /**
     * No Arg constructor used for FeedSources without labels
     */
    protected LabelSerializer() {
        super((Class<ArrayList<String>>) new ArrayList<String>().getClass());
    }

    /**
     * ArrayList Constructor used for FeedSources with labels
     */
    protected LabelSerializer(Class<ArrayList<String>> t) {
        super(t);
    }

    /**
     * Constructor used by Jackson to initialize Serializer
     */
    protected LabelSerializer(StdSerializer<?> src) {
        super(src);
    }

    /**
     * Method which converts List of Strings to List of Label objects
     * @param strings               List of IDs to create Label objects from
     * @param jsonGenerator         Jackson JsonGenerator
     * @param serializerProvider    Jackson SerializerProvider
     * @throws IOException
     */
    @Override
    public void serialize(ArrayList<String> strings, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartArray();
        // Each array item is a Label "object"
        for (String labelId: strings) {
            Label label = Persistence.labels.getById(labelId);
            // Use the default Label serializer
            jsonGenerator.writeObject(label);
        }
        jsonGenerator.writeEndArray();
    }
}