package com.conveyal.datatools;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.utils.HttpUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.DataManager.GTFS_DATA_SOURCE;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Base URL for application running during testing.
     */
    private static final String BASE_URL = "http://localhost:4000/";

    /**
     * @return input string with date appended
     */
    public static String appendDate(String inputString) {
        return String.join(" ", inputString, new Date().toString());
    }

    /**
     * Returns true only if an environment variable exists and is set to "true".
     */
    public static boolean getBooleanEnvVar (String var) {
        String variable = System.getenv(var);
        return variable != null && variable.equals("true");
    }

    /**
     * Checks whether the current environment appears to be a continuous integration environment.
     */
    public static boolean isCi () {
        return getBooleanEnvVar("CI");
    }

    /**
     * Parse a json string into an unmapped JsonNode object
     */
    public static JsonNode parseJson(String jsonString) throws IOException {
        return mapper.readTree(jsonString);
    }

    /**
     * Utility function to create a feed version during tests. Note: this is intended to run the job in the same thread,
     * so that tasks can run synchronously.
     */
    public static FeedVersion createFeedVersionFromGtfsZip(FeedSource source, String gtfsFileName) {
        File gtfsFile = new File(getGtfsResourcePath(gtfsFileName));
        return createFeedVersion(source, gtfsFile);
    }

    /**
     * Helper to get a File for the given file or folder that should be in the gtfs folder of the test resources
     */
    public static String getGtfsResourcePath(String gtfsFileName) {
        return TestUtils.class.getResource("gtfs/" + gtfsFileName).getFile();
    }

    /**
     * Utility function to create a feed version during tests. Note: this is intended to run the job in the same thread,
     * so that tasks can run synchronously.
     */
    public static FeedVersion createFeedVersion(FeedSource source, File gtfsFile) {
        FeedVersion version = new FeedVersion(source);
        InputStream is;
        try {
            is = new FileInputStream(gtfsFile);
            version.newGtfsFile(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Auth0UserProfile user = Auth0UserProfile.createTestAdminUser();
        ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(version, user, true);
        // Run in same thread to keep things synchronous.
        processSingleFeedJob.run();
        return version;
    }

    /**
     * Zip files in a folder into a temporary zip file
     * @return
     */
    public static File zipFolderFiles(String folderName) throws IOException {
        // create temporary zip file
        File tempFile = File.createTempFile("temp-gtfs-zip-", ".zip");
        tempFile.deleteOnExit();
        String folderPath = getGtfsResourcePath(folderName);
        // Do not nest files under a subdirectory if directly zipping a folder in src/main/resources
        compressZipfile(folderPath, tempFile.getAbsolutePath(), false);
        return tempFile;
    }

    /**
     * Compresses a folder into a zip file.
     *
     * @param sourceDir The directory to compress
     * @param outputFile The path to write the resulting zipfile to
     * @param nestDirectory whether nested folders should be preserved as subdirectories
     * @throws IOException
     */
    private static void compressZipfile(String sourceDir, String outputFile, boolean nestDirectory) throws IOException {
        ZipOutputStream zipFile = new ZipOutputStream(new FileOutputStream(outputFile));
        compressDirectoryToZipfile(sourceDir, sourceDir, zipFile, nestDirectory);
        IOUtils.closeQuietly(zipFile);
    }

    /**
     *
     * @param sourceDir The directory to compress
     * @param outputFile The path to write the resulting zipfile to
     * @param nestDirectory whether nested folders should be preserved as subdirectories
     */
    /**
     * Convenience method for zipping a directory.
     *
     * @param rootDir The root directory to zip
     * @param sourceDir The current directory of files to look through and add to the zip
     * @param out The ZipOutputStream to write to
     * @param nestDirectory Whether or not to preserve the nested directories in the zip file
     */
    private static void compressDirectoryToZipfile(String rootDir, String sourceDir, ZipOutputStream out, boolean nestDirectory) throws IOException {
        for (File file : new File(sourceDir).listFiles()) {
            if (file.isDirectory()) {
                compressDirectoryToZipfile(rootDir, fileNameWithDir(sourceDir, file.getName()), out, nestDirectory);
            } else {
                String folderName = sourceDir.replace(rootDir, "");
                String zipEntryName = nestDirectory
                    ? fileNameWithDir(folderName, file.getName())
                    : String.join("", folderName, file.getName());
                ZipEntry entry = new ZipEntry(zipEntryName);
                out.putNextEntry(entry);

                FileInputStream in = new FileInputStream(file.getAbsolutePath());
                IOUtils.copy(in, out);
                IOUtils.closeQuietly(in);
            }
        }
    }

    /**
     * Convenience method to join a directory path with a filename
     */
    public static String fileNameWithDir(String directory, String filename) {
        return String.join(File.separator, directory, filename);
    }

    /**
     * Asserts that the result of a SQL count statement is equal to an expected value
     *
     * @param sql A SQL statement in the form of `SELECT count(*) FROM ...`
     * @param expectedCount The expected count that is returned from the result of the SQL statement.
     */
    public static void assertThatSqlCountQueryYieldsExpectedCount(String sql, int expectedCount) throws SQLException {
        int count = -1;
        LOG.info(sql);
        // Encapsulate connection in try-with-resources to ensure it is closed and does not interfere with other tests.
        try (Connection connection = GTFS_DATA_SOURCE.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            while (resultSet.next()) {
                count = resultSet.getInt(1);
            }
        }
        assertThat(
            "Records matching query should equal expected count.",
            count,
            equalTo(expectedCount)
        );
    }

    /**
     * Asserts that there aren't any errors in a given namespace's errors table that match any of the given errorTypes
     *
     * @param namespace The namespace of the postgres database to look in
     * @param errorTypes Arguments of Strings of possible `error_type` values to check for
     */
    public static void assertThatFeedHasNoErrorsOfType (String namespace, String... errorTypes) throws SQLException {
        assertThatSqlCountQueryYieldsExpectedCount(
            String.format(
                "select count(*) from %s.errors where error_type in (%s)",
                namespace,
                Arrays.stream(errorTypes)
                    .map(error -> String.format("'%s'", error))
                    .collect(Collectors.joining(","))
            ),
            0
        );
    }

    /**
     * Send request to provided URL.
     */
    public static HttpResponse makeRequest(String path, String body, HttpUtils.REQUEST_METHOD requestMethod) {
        return HttpUtils.httpRequestRawResponse(
            URI.create(BASE_URL + path),
            1000,
            requestMethod,
            body
        );
    }

}
