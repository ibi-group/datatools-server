package com.conveyal.gtfs;

import com.conveyal.gtfs.model.Frequency;
import com.conveyal.gtfs.model.Pattern;
import com.conveyal.gtfs.model.Service;
import com.conveyal.gtfs.model.Shape;
import com.conveyal.gtfs.model.Trip;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import spark.Request;
import spark.Response;
import spark.ResponseTransformer;

import java.util.Map;

/**
 * Serve output as json.
 */

public class JsonTransformer implements ResponseTransformer {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String render(Object o) throws Exception {
        objectMapper.addMixIn(Trip.class, TripMixIn.class);
        objectMapper.addMixIn(Frequency.class, FrequencyMixIn.class);
        objectMapper.addMixIn(Pattern.class, PatternMixin.class);
        return objectMapper.writeValueAsString(o);
    }

    /** set the content type */
    public void type (Request request, Response response) {
        response.type("application/json");
    }

    public abstract class TripMixIn {
        @JsonIgnore public Map<Integer, Shape> shape_points;
        @JsonIgnore public Service service;
    }

    public abstract class FrequencyMixIn {
        @JsonIgnore public Trip trip;
    }

    public abstract class PatternMixin {
//        @JsonSerialize(using = GeometrySerializer.class)
//        @JsonDeserialize(using = GeometryDeserializer.class)
//        public LineString geometry;

        @JsonIgnore
        public Joiner joiner;
    }
}
