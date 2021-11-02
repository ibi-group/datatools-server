package com.conveyal.datatools.manager.models.transform;

import org.bson.codecs.pojo.annotations.BsonIgnore;

import java.io.Serializable;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * This class holds the regex/replacement pair, and the cached compiled regex pattern.
 */
public class Substitution implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Free-form text that describes the substitution. */
    public String description;
    /** The regex pattern string to match. */
    public String pattern;
    /** The string that should replace regex (see effectiveReplacement below). */
    public String replacement;
    /** true if whitespace surrounding regex should be create or normalized to one space. */
    public boolean normalizeSpace;

    /**
     * Whether this substitution object is invalid.
     * The `valid` property is read-only from the UI (see the empty setter below) and is not persisted to Mongo.
     * @return rue if the pattern for this substitution is invalid.
     */
    @BsonIgnore
    public boolean isValid() {
        try {
            initialize();
            return true;
        } catch (PatternSyntaxException pse) {
            return false;
        }
    }

    /**
     * This empty setter is needed to send the `valid` field to the UI
     * and to accept, without doing anything, substitution data from the UI that contain the `valid` field.
     * The `valid` property is read-only from the UI and is not persisted to Mongo.
     */
    @BsonIgnore
    public void setValid(boolean value) { }

    private Pattern patternObject;
    /**
     * Replacement string that is actually used for the substitution,
     * and set according to the value of normalizeSpace.
     */
    private String effectiveReplacement;

    /** Empty constructor needed for persistence */
    public Substitution() {}

    public Substitution(String pattern, String replacement) {
        this(pattern, replacement, false);
    }

    public Substitution(String pattern, String replacement, boolean normalizeSpace) {
        this.pattern = pattern;
        this.normalizeSpace = normalizeSpace;
        this.replacement = replacement;
    }

    /**
     * Pre-compiles the regex pattern and determines the actual replacement string
     * according to normalizeSpace.
     */
    private void initialize() {
        if (normalizeSpace) {
            // If normalizeSpace is set, reduce spaces before and after the regex to one space,
            // or insert one space before and one space after if there is none.
            // Note: if the replacement must be blank, then normalizeSpace should be set to false
            // and whitespace management should be handled in the regex instead.
            this.patternObject = Pattern.compile(String.format("\\s*%s\\s*", pattern));
            this.effectiveReplacement = String.format(" %s ", replacement);
        } else {
            this.patternObject = Pattern.compile(pattern);
            this.effectiveReplacement = replacement;
        }
    }

    /**
     * Perform the replacement of regex in the provided string, and return the result.
     */
    public String replaceAll(String input) {
        if (patternObject == null) {
            initialize();
        }
        return patternObject.matcher(input).replaceAll(effectiveReplacement);
    }
}
