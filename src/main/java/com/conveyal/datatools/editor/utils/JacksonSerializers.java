package com.conveyal.datatools.editor.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class JacksonSerializers {
    private static final Logger LOG = LoggerFactory.getLogger(JacksonSerializers.class);
    private static final BaseEncoding encoder = BaseEncoding.base64Url();

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
            String dateText = jp.getText();
            try {
                date = LocalDate.parse(dateText, DateTimeFormatter.BASIC_ISO_DATE);
                return date;
            } catch (Exception jsonException) {
                // This is here to catch any loads of database dumps that happen to have the old java.util.Date
                // field type in validationResult.  God help us.
                LOG.warn("Error parsing date value: `{}`, trying legacy java.util.Date date format", dateText);
                try {
                    date = Instant.ofEpochMilli(jp.getValueAsLong()).atZone(ZoneOffset.UTC).toLocalDate();
                    return date;
                } catch (Exception e) {
                    LOG.warn("Error parsing date value: `{}`", dateText);
                    e.printStackTrace();
                }
            }
            return null;
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

}
