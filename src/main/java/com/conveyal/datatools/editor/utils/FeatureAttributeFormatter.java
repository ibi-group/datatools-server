package com.conveyal.datatools.editor.utils;

import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FeatureAttributeFormatter {
    public static final Logger LOG = LoggerFactory.getLogger(FeatureAttributeFormatter.class);
    String formatString;
    Matcher matches;

    public FeatureAttributeFormatter(String format)
    {
        this.formatString = format;

        Pattern pattern = Pattern.compile("#([0-9]+)");

        this.matches = pattern.matcher(format);
    }

    public String format(SimpleFeature feature)
    {
        String output = new String(formatString);

        while(matches.find())
        {
            String sub = matches.group();

            Integer fieldPosition = Integer.parseInt(sub.replace("#", ""));

            try {
                String attributeString = feature.getAttribute(fieldPosition).toString();
                output = output.replace(sub, attributeString);
            } catch (Exception e) {
                LOG.warn("Index out of range.");
                e.printStackTrace();
            }

        }

        return output;
    }
}
