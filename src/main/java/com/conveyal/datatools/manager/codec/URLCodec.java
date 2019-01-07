package com.conveyal.datatools.manager.codec;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by landon on 9/6/17.
 */
public class URLCodec implements Codec<URL> {
    @Override
    public void encode(final BsonWriter writer, final URL value, final EncoderContext encoderContext) {
        writer.writeString(value.toString());
    }

    @Override
    public URL decode(final BsonReader reader, final DecoderContext decoderContext) {
        try {
            return new URL(reader.readString());
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Class<URL> getEncoderClass() {
        return URL.class;
    }
}
