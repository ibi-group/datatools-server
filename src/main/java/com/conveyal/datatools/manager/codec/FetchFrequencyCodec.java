package com.conveyal.datatools.manager.codec;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import com.conveyal.datatools.manager.models.FetchFrequency;

/**
 * Codec to help MongoDB decode/encode the {@link FetchFrequency} enum during de-/serialization.
 */
public class FetchFrequencyCodec implements Codec<FetchFrequency> {
    @Override
    public void encode(final BsonWriter writer, final FetchFrequency value, final EncoderContext encoderContext) {
        writer.writeString(value.toString());
    }

    @Override
    public FetchFrequency decode(final BsonReader reader, final DecoderContext decoderContext) {
        try {
            return FetchFrequency.fromValue(reader.readString());
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Class<FetchFrequency> getEncoderClass() {
        return FetchFrequency.class;
    }
}
