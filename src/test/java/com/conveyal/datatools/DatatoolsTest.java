package com.conveyal.datatools;

import com.conveyal.datatools.manager.DataManager;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by landon on 2/24/17.
 */
public abstract class DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(DatatoolsTest.class);
    private static boolean setUpIsDone = false;

    @BeforeAll
    public static void setUp() {
        if (setUpIsDone) {
            return;
        }
        LOG.info("DatatoolsTest setup");
        String[] args = {"configurations/default/env.yml.tmp", "configurations/default/server.yml.tmp"};
        try {
            DataManager.main(args);
            setUpIsDone = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
