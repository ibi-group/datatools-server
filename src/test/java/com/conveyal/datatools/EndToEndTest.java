package com.conveyal.datatools;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * This test suite runs the datatools-ui end-to-end tests in an effort to collect code coverage on
 * this library during those tests.  This test is only ran if the `RUN_E2E` environment variable
 * is set to `true`. This e2e test can be ran either locally or on CI.
 */
public class EndToEndTest {
    private static final Logger LOG = LoggerFactory.getLogger(EndToEndTest.class);
    private static final String datatoolsUiPath =
        Paths.get("").toAbsolutePath().resolveSibling("datatools-ui").toString();

    @BeforeAll
    public static void beforeAll () throws Exception {
        // make sure the RUN_E2E environment variable is set to true.  Otherwise, this test should
        // be skipped and the overall build should not depend on this test passing in order to save
        // time.
        assumeTrue(TestUtils.isRunningE2E());

        // all code after this point only runs with the assumption that the e2e test should be ran
        // check if additional setup should be done if in a CI environment.  It is assumed that a
        // dev instance of datatools-ui has already been manually started if this test is not being
        // ran in a CI environment.  It is also assumed that the datatools-ui directory is a sibling
        // directory to this installation and that it's possible to run the end-to-end command.
        if (TestUtils.isCi()) {
            // do extra setup for CI environment

            // clone dev datatools-ui
            runCommand(
                String.format(
                    "git clone https://github.com/ibi-group/datatools-ui.git %s",
                    datatoolsUiPath
                )
            );

            // install all needed node modules
            runCommand(String.format("yarn --cwd %s", datatoolsUiPath));
        }

        // start the database server
        DatatoolsTest.setUp();
    }

    @Test
    public void endToEndTest () throws Exception {
        // run the end-to-end test and wait for it to finish
        runCommand(String.format("npm run test-end-to-end --prefix %s", datatoolsUiPath));
    }

    /**
     * Run a shell command using bash. The command will be ran and the output of the command will be logged. If the
     * command fails to run successfully an error is thrown.
     */
    public static void runCommand (String command) throws RuntimeException, IOException, InterruptedException {
        LOG.info(String.format("Running command: %s", command));

        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("bash", "-c", command);
        Process process = processBuilder.start();

        // capture and log output
        LOG.info("stdout:");
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

        String line;
        while ((line = reader.readLine()) != null) {
            LOG.info(line);
        }

        LOG.info("stderr:");
        reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));

        while ((line = reader.readLine()) != null) {
            LOG.error(line);
        }

        int exitVal = process.waitFor();
        if (exitVal == 0) {
            LOG.info(String.format("Command %s completed successfully", command));
        } else {
            String failureMessage = String.format("Command %s failed", command);
            LOG.error(failureMessage);
            throw new RuntimeException(failureMessage);
        }
    }
}
