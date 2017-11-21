package com.conveyal.datatools.editor.controllers;

import com.conveyal.datatools.editor.models.transit.GtfsRouteType;
import com.conveyal.datatools.editor.models.transit.TripDirection;
import com.conveyal.geojson.GeoJsonModule;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.time.LocalDate;

import com.conveyal.datatools.editor.utils.JacksonSerializers;

import java.io.IOException;
import java.io.StringWriter;

public class Base {
    public static ObjectMapper mapper = new ObjectMapper();
    private static JsonFactory jf = new JsonFactory();
    
    static {
        SimpleModule mod = new SimpleModule();
        mod.addDeserializer(LocalDate.class, new JacksonSerializers.LocalDateDeserializer());
        mod.addSerializer(LocalDate.class, new JacksonSerializers.LocalDateSerializer());
        mod.addDeserializer(GtfsRouteType.class, new JacksonSerializers.GtfsRouteTypeDeserializer());
        mod.addSerializer(GtfsRouteType.class, new JacksonSerializers.GtfsRouteTypeSerializer());
        mapper.getSerializerProvider().setNullKeySerializer(new JacksonSerializers.MyDtoNullKeySerializer());
        mapper.registerModule(mod);
        mapper.registerModule(new GeoJsonModule());
    }

    public static String toJson(Object pojo, boolean prettyPrint)
            throws IOException {
                StringWriter sw = new StringWriter();
                JsonGenerator jg = jf.createGenerator(sw);
                if (prettyPrint) {
                    jg.useDefaultPrettyPrinter();
                }
                mapper.writeValue(jg, pojo);
                return sw.toString();
    }
}