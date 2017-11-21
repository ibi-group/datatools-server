package com.conveyal.datatools.editor.utils;

import com.conveyal.datatools.editor.models.transit.GtfsRouteType;
import com.conveyal.datatools.editor.models.transit.TripDirection;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.io.BaseEncoding;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.mapdb.Fun.Tuple2;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class JacksonSerializers {
    private static final BaseEncoding encoder = BaseEncoding.base64Url();

    public static class Tuple2Serializer extends StdScalarSerializer<Tuple2<String, String>> {
        private static final long serialVersionUID = 884752482339455539L;

        public Tuple2Serializer () {
            super(Tuple2.class, true);
        }

        @Override
        public void serialize(Tuple2<String, String> t2, JsonGenerator jgen,
                              SerializerProvider arg2) throws IOException {
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
        private static final long serialVersionUID = -9155687065800376769L;

        public Tuple2Deserializer () {
            super(Tuple2.class);
        }

        @Override
        public Tuple2<String, String> deserialize(JsonParser jp,
                                                  DeserializationContext arg1) throws IOException {
            return deserialize(jp.readValueAs(String.class));
        }

        public static Tuple2<String, String> deserialize (String serialized) throws IOException {
            String[] val = serialized.split(":");
            if (val.length != 2) {
                throw new IOException("Unable to parse value");
            }

            return new Tuple2<>(new String(encoder.decode(val[0]), "UTF-8"), new String(encoder.decode(val[1]), "UTF-8"));
        }
    }

    public static class Tuple2IntSerializer extends StdScalarSerializer<Tuple2<String, Integer>> {
        private static final long serialVersionUID = 3201085724165980819L;

        public Tuple2IntSerializer () {
            super(Tuple2.class, true);
        }

        @Override
        public void serialize(Tuple2<String, Integer> t2, JsonGenerator jgen,
                              SerializerProvider arg2) throws IOException {
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
        private static final long serialVersionUID = -6787630225359327452L;

        public Tuple2IntDeserializer () {
            super(Tuple2.class);
        }

        @Override
        public Tuple2<String, Integer> deserialize(JsonParser jp, DeserializationContext arg1) throws IOException {
            return deserialize(jp.readValueAs(String.class));
        }

        public static Tuple2<String, Integer> deserialize (String serialized) throws IOException {
            String[] val = serialized.split(":");
            if (val.length != 2) {
                throw new IOException("Unable to parse value");
            }

            return new Tuple2<>(new String(encoder.decode(val[0]), "UTF-8"), Integer.parseInt(val[1]));
        }
    }

    /** serialize local dates as noon GMT epoch times */
    public static class LocalDateSerializer extends StdScalarSerializer<LocalDate> {
        private static final long serialVersionUID = 3153194744968260324L;

        public LocalDateSerializer() {
            super(LocalDate.class, false);
        }

        @Override
        public void serialize(LocalDate ld, JsonGenerator jgen, SerializerProvider arg2) throws IOException {
            // YYYYMMDD
            jgen.writeString(ld.format(DateTimeFormatter.BASIC_ISO_DATE));
        }
    }

    /** deserialize local dates from GMT epochs */
    public static class LocalDateDeserializer extends StdScalarDeserializer<LocalDate> {
        private static final long serialVersionUID = -1855560624079270379L;

        public LocalDateDeserializer () {
            super(LocalDate.class);
        }

        @Override
        public LocalDate deserialize(JsonParser jp, DeserializationContext arg1) throws IOException {
            LocalDate date;
            try {
                date = LocalDate.parse(jp.getText(), DateTimeFormatter.BASIC_ISO_DATE);
                return date;
            } catch (Exception jsonException) {
                // This is here to catch any loads of database dumps that happen to have the old java.util.Date
                // field type in validationResult.  God help us.
                System.out.println("Error parsing date value, trying legacy java.util.Date date format");
                try {
                    date = Instant.ofEpochMilli(jp.getValueAsLong()).atZone(ZoneOffset.UTC).toLocalDate();
                    return date;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

//            System.out.println(jp.getValueAsLong());
//            System.out.println(date.format(DateTimeFormatter.BASIC_ISO_DATE));
            return null;
        }
    }

    /** serialize GtfsRouteType as GTFS integer value */
    public static class GtfsRouteTypeSerializer extends StdScalarSerializer<GtfsRouteType> {
        private static final long serialVersionUID = -8179814233698591433L;

        public GtfsRouteTypeSerializer() {
            super(GtfsRouteType.class, false);
        }

        @Override
        public void serialize(GtfsRouteType gtfsRouteType, JsonGenerator jsonGenerator,
                              SerializerProvider arg2) throws IOException {
            jsonGenerator.writeNumber(gtfsRouteType.toGtfs());
        }
    }

    /** serialize GTFS integer value  to GtfsRouteType */
    public static class GtfsRouteTypeDeserializer extends StdScalarDeserializer<GtfsRouteType> {
        private static final long serialVersionUID = 2771914080477037467L;

        public GtfsRouteTypeDeserializer () {
            super(GtfsRouteType.class);
        }

        @Override
        public GtfsRouteType deserialize(JsonParser jp,
                                     DeserializationContext arg1) throws IOException {
            return GtfsRouteType.fromGtfs(jp.getValueAsInt());
        }
    }

    public static class MyDtoNullKeySerializer extends StdSerializer<Object> {
        private static final long serialVersionUID = -8104007875350340832L;

        public MyDtoNullKeySerializer() {
            this(null);
        }

        public MyDtoNullKeySerializer(Class<Object> t) {
            super(t);
        }

        @Override
        public void serialize(Object nullKey, JsonGenerator jsonGenerator, SerializerProvider unused)
                throws IOException {
            jsonGenerator.writeFieldName("");
        }
    }

    public static final DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** Serialize a local date to an ISO date (year-month-day) */
    public static class LocalDateIsoSerializer extends StdScalarSerializer<LocalDate> {
        private static final long serialVersionUID = 6365116779135936730L;

        public LocalDateIsoSerializer () {
            super(LocalDate.class, false);
        }

        @Override
        public void serialize(LocalDate localDate, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeString(localDate.format(format));
        }
    }

    /** Deserialize an ISO date (year-month-day) */
    public static class LocalDateIsoDeserializer extends StdScalarDeserializer<LocalDate> {
        private static final long serialVersionUID = -1703584495462802108L;

        public LocalDateIsoDeserializer () {
            super(LocalDate.class);
        }

        @Override
        public LocalDate deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            return LocalDate.parse(jsonParser.getValueAsString(), format);
        }

    }
}
