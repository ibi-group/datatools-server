package com.conveyal.datatools.manager.codec;

import org.bson.BSONException;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.awt.geom.Rectangle2D;

/**
 * Created by landon on 9/6/17.
 */
public class Rectangle2DCodec implements Codec<Rectangle2D> {
    @Override
    public void encode(final BsonWriter writer, final Rectangle2D value, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        writer.writeDouble("north", value.getMaxY());
        writer.writeDouble("south", value.getMinY());
        writer.writeDouble("east", value.getMaxX());
        writer.writeDouble("west", value.getMinX());
        writer.writeEndDocument();
    }

    @Override
    public Rectangle2D decode(final BsonReader reader, final DecoderContext decoderContext) {
        Double north = null, south = null, east = null, west = null;
        reader.readStartDocument();

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            switch (fieldName) {
                case "north":
                    north = reader.readDouble();
                    break;
                case "south":
                    south = reader.readDouble();
                    break;
                case "east":
                    east = reader.readDouble();
                    break;
                case "west":
                    west = reader.readDouble();
                    break;
            }
        }
        reader.readEndDocument();

        if (north == null || south == null || east == null || west == null)
            throw new BSONException("Unable to deserialize bounding box; need north, south, east, and west.");

        Rectangle2D.Double ret = new Rectangle2D.Double(west, north, 0, 0);
        ret.add(east, south);
        return ret;
    }

    @Override
    public Class<Rectangle2D> getEncoderClass() {
        return Rectangle2D.class;
    }

    /**
     * A place to hold information from the JSON stream temporarily.
     */
    private static class IntermediateBoundingBox {
        public Double north;
        public Double south;
        public Double east;
        public Double west;
    }
}
