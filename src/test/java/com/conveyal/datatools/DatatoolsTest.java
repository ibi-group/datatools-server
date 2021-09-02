package com.conveyal.datatools;

import com.conveyal.datatools.manager.DataManager;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This abstract class contains is used to start a test instance of datatools-server that other tests can use to perform
 * various tests.
 *
 * A majority of the test involves setting up the proper config files for performing tests - especially the e2e tests.
 */
public abstract class DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(DatatoolsTest.class);
    private static boolean setUpIsDone = false;
    private static final Yaml yaml = new Yaml(); // needed for writing config files on CI for the e2e tests

    @BeforeAll
    public static void setUp() throws RuntimeException, IOException {
        if (setUpIsDone) {
            return;
        }
        LOG.info("DatatoolsTest setup");

        setupConfigFiles();

        // If in the e2e environment, use the secret env.yml and server.yml files to start the server. When run on
        // CI, these files will automatically be setup.
        String[] args = TestUtils.isRunningE2E()
            ? new String[] { "configurations/default/env.yml", "configurations/default/server.yml" }
            : new String[] { "configurations/test/env.yml.tmp", "configurations/test/server.yml.tmp" };

        // fail this test and others if the above files do not exist
        for (String arg : args) {
            File f = new File(arg);
            if (!f.exists() || f.isDirectory()) {
                throw new IOException(String.format("Required config file %s does not exist!", f.getName()));
            }
        }

        // If NOT in the e2e environment, ensure that /tmp/gtfsplus folder exists
        // (otherwise some unit tests will be skipped).
        if (!TestUtils.isRunningE2E()) {
            File f = new File("/tmp/gtfsplus");
            if (f.exists() && !f.isDirectory()) {
                f.delete();
            }
            if (!f.exists()) {
                f.mkdirs();
            }
        }

        LOG.info("Starting server");
        try {
            DataManager.main(args);
            setUpIsDone = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates config files for datatools-server if being run on CI and the e2e environment variable is activated.
     * These special config files are needed in order to run e2e tests. Some of the config files contain sensitive
     * information that cannot be checked into the repo, so that data is obtained from environment variables in CI.
     */
    private static void setupConfigFiles() throws RuntimeException, IOException {
        if (TestUtils.isCi() && TestUtils.isRunningE2E()) {
            LOG.info("Setting up server in CI environment");

            // first make sure all the necessary environment varaibles are defined
            String[] requiredEnvVars = {
                "AUTH0_CLIENT_ID",
                "AUTH0_DOMAIN",
                "AUTH0_SECRET",
                "AUTH0_API_CLIENT",
                "AUTH0_API_SECRET",
                "OSM_VEX",
                "SPARKPOST_EMAIL",
                "SPARKPOST_KEY",
                "S3_BUCKET",
                "TRANSITFEEDS_KEY"
            };

            List<String> missingVars = new ArrayList<>();
            for (String requiredEnvVar : requiredEnvVars) {
                if (System.getenv(requiredEnvVar) == null) {
                    missingVars.add(requiredEnvVar);
                }
            }

            if (missingVars.size() > 0) {
                throw new RuntimeException(
                    String.format(
                        "Required environment variables missing: %s",
                        String.join(", ", missingVars)
                    )
                );
            }

            // create config files based off of default files plus some sensitive environment
            // variables

            // load template file contents
            LOG.info("Loading YAML config templates");
            String envTemplateFile = "configurations/default/env.yml.tmp";

            Map envConfig = yaml.load(
                new FileInputStream(new File(envTemplateFile))
            );

            String serverTemplateFile = "configurations/default/server.yml.tmp";
            Map serverConfig = yaml.load(
                new FileInputStream(new File(serverTemplateFile))
            );

            // write sensitive info to the config files
            // env.yml
            String[] envVars = {
                "AUTH0_CLIENT_ID",
                "AUTH0_DOMAIN",
                "AUTH0_SECRET",
                "AUTH0_API_CLIENT",
                "AUTH0_API_SECRET",
                "OSM_VEX",
                "SPARKPOST_KEY",
                "SPARKPOST_EMAIL"
            };
            for (String envVar : envVars) {
                envConfig.put(envVar, System.getenv(envVar));
            }

            // server.yml
            String s3Bucket = System.getenv("S3_BUCKET");
            String transitFeedsKey = System.getenv("TRANSITFEEDS_KEY");

            Map applicationConfig = (Map) serverConfig.get("application");
            Map extensionsConfig = (Map) serverConfig.get("extensions");
            Map applicationDataConfig = (Map) applicationConfig.get("data");
            Map transitFeedsConfig = (Map) extensionsConfig.get("transitfeeds");

            applicationConfig.put("client_assets_url", "http://localhost:4000");
            applicationDataConfig.put("gtfs_s3_bucket", s3Bucket);
            // make sure use_s3_storage is true so it's possible to upload/download feeds from s3
            applicationDataConfig.put("use_s3_storage", true);
            transitFeedsConfig.put("key", transitFeedsKey);

            // write files to disk
            LOG.info("Writing YAML config files");
            yaml.dump(envConfig, new FileWriter("configurations/default/env.yml"));
            yaml.dump(serverConfig, new FileWriter("configurations/default/server.yml"));
        }
    }
}
