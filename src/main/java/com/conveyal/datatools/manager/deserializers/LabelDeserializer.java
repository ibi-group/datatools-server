package com.conveyal.datatools.manager.deserializers;

import com.conveyal.datatools.manager.models.Label;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.util.ArrayList;

public class LabelDeserializer extends StdDeserializer<ArrayList<Label>> {
    public LabelDeserializer() {
        this(null);
    }

    public LabelDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public ArrayList<Label> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        // Input is an array of strings which are IDs of Labels
        ArrayNode arrayNode = jsonParser.getCodec().readTree(jsonParser);

        // Create list to put matched labels into
        ArrayList<Label> labels = new ArrayList<Label>();

        if (arrayNode.isArray()) {
            for (JsonNode jsonNode : arrayNode) {
                // Process every node as a string
                String id = jsonNode.asText();

                Label matchedLabel = Persistence.labels.getById(id);
                if (matchedLabel == null) {
                    throw new IOException("Label with ID " + id + " does not exist!");
                }

                labels.add(Persistence.labels.getById(id));
            }
        }

        return labels;
    }
}