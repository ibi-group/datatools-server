package com.conveyal.datatools.editor.utils;

import com.conveyal.datatools.editor.models.transit.GtfsRouteType;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;
import com.google.common.io.BaseEncoding;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.LocalDate;
import java.time.LocalTime;
//import java.time.format.D;
import java.time.format.DateTimeFormatter;
import org.mapdb.Fun.Tuple2;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class JacksonSerializers {
    private static final BaseEncoding encoder = BaseEncoding.base64Url();
    public static class Tuple2Serializer extends StdScalarSerializer<Tuple2<String, String>> {
        public Tuple2Serializer () {
            super(Tuple2.class, true);
        }

        @Override
        public void serialize(Tuple2<String, String> t2, JsonGenerator jgen,
                              SerializerProvider arg2) throws IOException,
                JsonProcessingException {
            jgen.writeString(serialize(t2));
        }

        public static String serialize (Tuple2<String, String> t2) {
            try {
                return encoder.encode(t2.a.getBytes("UTF-8")) + ":" + encoder.encode(t2.b.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new UnsupportedOperationException(e);
            }
        }
    }

    public static class Tuple2Deserializer extends StdScalarDeserializer<Tuple2<String, String>> {
        public Tuple2Deserializer () {
            super(Tuple2.class);
        }

        @Override
        public Tuple2<String, String> deserialize(JsonParser jp,
                                                  DeserializationContext arg1) throws IOException,
                JsonProcessingException {
            return deserialize(jp.readValueAs(String.class));
        }

        public static Tuple2<String, String> deserialize (String serialized) throws IOException {
            String[] val = serialized.split(":");
            if (val.length != 2) {
                throw new IOException("Unable to parse value");
            }

            return new Tuple2<String, String>(new String(encoder.decode(val[0]), "UTF-8"), new String(encoder.decode(val[1]), "UTF-8"));
        }
    }

    public static class Tuple2IntSerializer extends StdScalarSerializer<Tuple2<String, Integer>> {
        public Tuple2IntSerializer () {
            super(Tuple2.class, true);
        }

        @Override
        public void serialize(Tuple2<String, Integer> t2, JsonGenerator jgen,
                              SerializerProvider arg2) throws IOException,
                JsonProcessingException {
            jgen.writeString(serialize(t2));
        }

        public static String serialize (Tuple2<String, Integer> t2) {
            try {
                return encoder.encode(t2.a.getBytes("UTF-8")) + ":" + t2.b.toString();
            } catch (UnsupportedEncodingException e) {
                throw new UnsupportedOperationException(e);
            }
        }
    }

    public static class Tuple2IntDeserializer extends StdScalarDeserializer<Tuple2<String, Integer>> {
        public Tuple2IntDeserializer () {
            super(Tuple2.class);
        }

        @Override
        public Tuple2<String, Integer> deserialize(JsonParser jp,
                                                   DeserializationContext arg1) throws IOException,
                JsonProcessingException {
            return deserialize(jp.readValueAs(String.class));
        }

        public static Tuple2<String, Integer> deserialize (String serialized) throws IOException {
            String[] val = serialized.split(":");
            if (val.length != 2) {
                throw new IOException("Unable to parse value");
            }

            return new Tuple2<String, Integer>(new String(encoder.decode(val[0]), "UTF-8"), Integer.parseInt(val[1]));
        }
    }

    /** serialize local dates as noon GMT epoch times */
    public static class LocalDateSerializer extends StdScalarSerializer<LocalDate> {
        public LocalDateSerializer() {
            super(LocalDate.class, false);
        }

        @Override
        public void serialize(LocalDate ld, JsonGenerator jgen,
                              SerializerProvider arg2) throws IOException,
                JsonGenerationException {
            long millis = ld.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            jgen.writeNumber(millis);
        }
    }

    /** deserialize local dates from GMT epochs */
    public static class LocalDateDeserializer extends StdScalarDeserializer<LocalDate> {
        public LocalDateDeserializer () {
            super(LocalDate.class);
        }

        @Override
        public LocalDate deserialize(JsonParser jp,
                                     DeserializationContext arg1) throws IOException,
                JsonProcessingException {

            LocalDate date = Instant.ofEpochMilli(jp.getValueAsLong()).atZone(ZoneOffset.UTC).toLocalDate();
            return date;
        }
    }

    /** serialize local dates as noon GMT epoch times */
    public static class GtfsRouteTypeSerializer extends StdScalarSerializer<GtfsRouteType> {
        public GtfsRouteTypeSerializer() {
            super(GtfsRouteType.class, false);
        }

        @Override
        public void serialize(GtfsRouteType gtfsRouteType, JsonGenerator jgen,
                              SerializerProvider arg2) throws IOException,
                JsonGenerationException {
            jgen.writeNumber(gtfsRouteType.toGtfs());
        }
    }

    /** deserialize local dates from GMT epochs */
    public static class GtfsRouteTypeDeserializer extends StdScalarDeserializer<GtfsRouteType> {
        public GtfsRouteTypeDeserializer () {
            super(GtfsRouteType.class);
        }

        @Override
        public GtfsRouteType deserialize(JsonParser jp,
                                     DeserializationContext arg1) throws IOException,
                JsonProcessingException {
            return GtfsRouteType.fromGtfs(jp.getValueAsInt());
        }
    }

    public static final DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** Serialize a local date to an ISO date (year-month-day) */
    public static class LocalDateIsoSerializer extends StdScalarSerializer<LocalDate> {
        public LocalDateIsoSerializer () {
            super(LocalDate.class, false);
        }

        @Override
        public void serialize(LocalDate localDate, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonGenerationException {
            jsonGenerator.writeString(localDate.format(format));
        }
    }

    /** Deserialize an ISO date (year-month-day) */
    public static class LocalDateIsoDeserializer extends StdScalarDeserializer<LocalDate> {
        public LocalDateIsoDeserializer () {
            super(LocalDate.class);
        }

        @Override
        public LocalDate deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            return LocalDate.parse(jsonParser.getValueAsString(), format);
        }

    }
}
