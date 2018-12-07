package com.conveyal.datatools.manager.utils.json;

import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Map;

import com.conveyal.datatools.editor.models.transit.GtfsRouteType;
import com.conveyal.datatools.editor.utils.JacksonSerializers;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

/**
 * Helper methods for writing REST API routines
 * @author mattwigway
 *
 */
public class JsonManager<T> {
    private ObjectWriter ow;
    private ObjectMapper om;

    /**
     * Create a new JsonManager
     * @param theClass The class to create a json manager for (yes, also in the diamonds).
     * @param view The view to use
     */
    public JsonManager (Class<T> theClass, Class view) {
        this.theClass = theClass;
        this.om = new ObjectMapper();
        // previous model for gtfs validation errors
//        om.addMixIn(InvalidValue.class, InvalidValueMixIn.class);
        om.addMixIn(Rectangle2D.class, Rectangle2DMixIn.class);
        SimpleModule deser = new SimpleModule();

        deser.addDeserializer(LocalDate.class, new JacksonSerializers.LocalDateDeserializer());
        deser.addSerializer(LocalDate.class, new JacksonSerializers.LocalDateSerializer());

        deser.addDeserializer(GtfsRouteType.class, new JacksonSerializers.GtfsRouteTypeDeserializer());
        deser.addSerializer(GtfsRouteType.class, new JacksonSerializers.GtfsRouteTypeSerializer());

        deser.addDeserializer(Rectangle2D.class, new Rectangle2DDeserializer());
        om.registerModule(deser);
        om.getSerializerProvider().setNullKeySerializer(new JacksonSerializers.MyDtoNullKeySerializer());
//        om.registerModule(new JavaTimeModule());
        SimpleFilterProvider filters = new SimpleFilterProvider();
        filters.addFilter("bbox", SimpleBeanPropertyFilter.filterOutAllExcept("west", "east", "south", "north"));
        this.ow = om.writer(filters).withView(view);
    }

    private Class<T> theClass;

    /**
     * Add an additional mixin for serialization with this object mapper.
     */
    public void addMixin(Class target, Class mixin) {
        om.addMixIn(target, mixin);
    }

    public String write(Object o) throws JsonProcessingException{
        if (o instanceof String) {
            return (String) o;
        }
        return ow.writeValueAsString(o);
    }

    /**
     * Convert an object to its JSON representation
     * @param o the object to convert
     * @return the JSON string
     * @throws JsonProcessingException
     */
    /*public String write (T o) throws JsonProcessingException {
        return ow.writeValueAsString(o);
    }*/

    /**
     * Convert a collection of objects to their JSON representation.
     * @param c the collection
     * @return A JsonNode representing the collection
     * @throws JsonProcessingException
     */
    public String write (Collection<T> c) throws JsonProcessingException {
        return ow.writeValueAsString(c);
    }

    public String write (Map<String, T> map) throws JsonProcessingException {
        return ow.writeValueAsString(map);
    }

    public T read (String s) throws JsonParseException, JsonMappingException, IOException {
        return om.readValue(s, theClass);
    }

    public T read (JsonParser p) throws JsonParseException, JsonMappingException, IOException {
        return om.readValue(p, theClass);
    }

    public T read(JsonNode asJson) {
        return om.convertValue(asJson, theClass);
    }
}
