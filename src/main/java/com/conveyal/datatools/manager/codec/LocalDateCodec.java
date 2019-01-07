package com.conveyal.datatools.manager.codec;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Created by landon on 9/6/17.
 */
public class LocalDateCodec implements Codec<LocalDate> {
    private static final Logger LOG = LoggerFactory.getLogger(LocalDateCodec.class);
    @Override
    public void encode(final BsonWriter writer, final LocalDate value, final EncoderContext encoderContext) {
        writer.writeString(value.format(DateTimeFormatter.BASIC_ISO_DATE));
    }

    @Override
    public LocalDate decode(final BsonReader reader, final DecoderContext decoderContext) {
        LocalDate date;
        try {
            date = LocalDate.parse(reader.readString(), DateTimeFormatter.BASIC_ISO_DATE);
            return date;
        } catch (Exception jsonException) {
            // This is here to catch any loads of database dumps that happen to have the old java.util.Date
            // field type in validationResult. God help us.
            LOG.error("Error parsing date value, trying legacy java.util.Date date format");
            try {
                date = Instant.ofEpochMilli(reader.readInt64()).atZone(ZoneOffset.UTC).toLocalDate();
                return date;
            } catch (Exception e) {
                LOG.error("Error parsing date value with legacy java.util.Date date format");
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public Class<LocalDate> getEncoderClass() {
        return LocalDate.class;
    }
}