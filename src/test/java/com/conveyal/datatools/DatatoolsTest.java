package com.conveyal.datatools;

import com.conveyal.datatools.manager.DataManager;
import org.junit.BeforeClass;
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

import static com.conveyal.datatools.TestUtils.isCi;

/**
 * Created by landon on 2/24/17.
 */
public abstract class DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(DatatoolsTest.class);
    private static boolean setUpIsDone = false;
    private static final Yaml yaml = new Yaml();

    @BeforeClass
    public static void setUp() {
        if (setUpIsDone) {
            return;
        }
        LOG.info("DatatoolsTest setup");

        if (isCi()) {
            LOG.info("Setting up server in CI environment");

            // first make sure all the necessary environment varaibles are defined
            String[] requiredEnvVars = {
                "AUTH0_CLIENT_ID",
                "AUTH0_DOMAIN",
                "AUTH0_SECRET",
                "AUTH0_TOKEN",
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
                throw new Exception(
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
                "AUTH0_TOKEN",
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

            applicationConfig.put("assets_bucket", s3Bucket);
            applicationDataConfig.put("gtfs_s3_bucket", s3Bucket);
            transitFeedsConfig.put("key", transitFeedsKey);

            // write files to disk
            LOG.info("Writing YAML config files");
            yaml.dump(envConfig, new FileWriter("configurations/default/env.yml"));
            yaml.dump(serverConfig, new FileWriter("configurations/default/server.yml"));
        }

        // This test (and all those that depend on it) assume that the env.yml and server.yml
        // files have been properly configured.  In a CI environment, these files are setup
        // automatically, but the setup must be done manually locally.
        String[] args = {"configurations/default/env.yml", "configurations/default/server.yml"};

        // fail this test and others if the above files do not exist
        for (String arg : args) {
            File f = new File(arg);
            if (!f.exists() || f.isDirectory()) {
                throw new Exception(String.format("Required config file %s does not exist!"));
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
}
