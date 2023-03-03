package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.*;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.google.common.io.Files;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class DeploymentGisExportJobTest extends GisExportJobTest {
    private static final Logger LOG = LoggerFactory.getLogger(DeploymentGisExportJobTest.class);
    private static Deployment deployment;
    private static Auth0UserProfile user;

    /** Unzip the shapefile into a temp directory and return a list of its files (folders in the Deployment test case). */
    private File[] getFoldersFromZippedShapefile(File zipFile) throws IOException {
        File destDir = Files.createTempDir();
        ZipFile zippedFolder = new ZipFile(zipFile);
        Enumeration<? extends ZipEntry> e = zippedFolder.entries();
        while (e.hasMoreElements()) {
            ZipEntry entry = e.nextElement();
            File destinationPath = new File(destDir, entry.getName());
            if (!destinationPath.toPath().normalize().startsWith(destDir.toPath().normalize())) {
                throw new RuntimeException("Bad zip entry");
            }
            // Create parent directories
            destinationPath.getParentFile().mkdirs();

            if (!entry.isDirectory()) {
                System.out.println("Extracting file: " + destinationPath);
                BufferedInputStream bis = new BufferedInputStream(zippedFolder.getInputStream(entry));

                int b;
                byte buffer[] = new byte[1024];

                FileOutputStream fos = new FileOutputStream(destinationPath);
                BufferedOutputStream bos = new BufferedOutputStream(fos, 1024);

                while ((b = bis.read(buffer, 0, 1024)) != -1) {
                    bos.write(buffer, 0, b);
                }

                bos.close();
                bis.close();
            }
        }
        return destDir.listFiles();
    }

    @BeforeAll
    public static void setUp() throws IOException {
        DatatoolsTest.setUp();
        // Calling GisExportJobTest.setUp() creates project and feeds
        GisExportJobTest.setUp();
        user = Auth0UserProfile.createTestAdminUser();
        deployment = new Deployment(project);
        deployment.feedVersionIds.add(calTrainVersion.id);
        deployment.feedVersionIds.add(hawaiiVersion.id);
        Persistence.deployments.create(deployment);
    }

    /**
     * Ensures that shapefiles containing stop features for a deployment can be exported and
     * contain geometry for each stop.
     */
    @Override
    @Test
    public void canExportStops() throws IOException, SQLException {
        File zipFile = File.createTempFile("stops", ".zip");

        DeploymentGisExportJob gisExportJob = new DeploymentGisExportJob(GisExportJob.ExportType.STOPS, deployment, zipFile, user);
        gisExportJob.run();
        assertThat(gisExportJob.status.error, equalTo(false));

        File[] folders = getFoldersFromZippedShapefile(zipFile);
        for (File file : folders) {
            File[] shapefileFiles = file.listFiles();
            String agencyName = file.getName();
            validateShapefiles(shapefileFiles, agencyName, GisExportJob.ExportType.STOPS);
        }
    }

    /**
     * Ensures that shapefiles containing route (pattern) features for a deployment can be
     * exported and contain geometry for each pattern (includes checks for exporting shapes from
     * pattern stops).
     */
    @Override
    @Test
    public void canExportRoutes() throws IOException, SQLException {
        File zipFile = File.createTempFile("routes", ".zip");

        DeploymentGisExportJob gisExportJob = new DeploymentGisExportJob(GisExportJob.ExportType.ROUTES, deployment, zipFile, user);
        gisExportJob.run();
        assertThat(gisExportJob.status.error, equalTo(false));

        File[] folders = getFoldersFromZippedShapefile(zipFile);
        for (File file : folders) {
            File[] shapefileFiles = file.listFiles();
            String agencyName = file.getName();
            validateShapefiles(shapefileFiles, agencyName, GisExportJob.ExportType.ROUTES);
        }
    }

    /**
     * We override the Pattern Stop export to do nothing since it is implicitly checked within the
     * canExportRoutes hawaiiVersion checks above.
     */
    @Override
    @Test
    public void canExportRoutesFromPatternStops() {}

}
