package com.conveyal.datatools.manager.models.transform;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SubstitutionTest {
    @Test
    public void shouldNotFlagValidPattern() {
        Substitution substitution = new Substitution("\\bCir\\b", "Circle");
        assertTrue(substitution.isValid());
    }

    @Test
    public void shouldFlagInvalidPattern() {
        Substitution substitution = new Substitution("\\Cir\\b", "Circle");
        assertFalse(substitution.isValid());
    }
}
