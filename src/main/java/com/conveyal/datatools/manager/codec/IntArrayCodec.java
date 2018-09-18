package com.conveyal.datatools.manager.codec;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class IntArrayCodec implements Codec<int[]> {

    @Override
    public void encode(final BsonWriter writer, final int[] value, final EncoderContext encoderContext) {
        writer.writeStartArray();
        for (int v : value) {
            writer.writeInt32(v);
        }
        writer.writeEndArray();
    }

    @Override
    public int[] decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartArray();

        TIntList intList = new TIntArrayList();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            intList.add(reader.readInt32());
        }

        reader.readEndArray();
        return intList.toArray();
    }

    @Override
    public Class<int[]> getEncoderClass() {
        return int[].class;
    }
}
