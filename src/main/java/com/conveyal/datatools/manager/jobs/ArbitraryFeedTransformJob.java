package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedTransformation;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.google.common.io.ByteStreams;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public class ArbitraryFeedTransformJob extends MonitorableJob {

    private final FeedVersion target;
    private final FeedTransformation transformation;

    public ArbitraryFeedTransformJob(Auth0UserProfile owner, FeedVersion target, FeedTransformation transformation) {
        super(owner, "", JobType.ARBITRARY_FEED_TRANSFORM);
        this.target = target;
        this.transformation = transformation;
    }

    @Override
    public void jobLogic() throws Exception {
        switch (transformation.transformType) {
            // FIXME Should non-transform jobs be executed here? Perhaps not. This might make more sense to execute at
            //  the project level. The one job in particular I'm thinking of that we might want to run after a feed load
            //  would be to auto-deploy to OTP after a successful feed load. We may want
            // case RUN_JOB: addNextJob();
            // This transformation operates at the zip file level and will run before the GTFS file is loaded.
            case REPLACE_FILE_FROM_VERSION: {
                // TODO: Refactor into validation code?
                FeedVersion sourceVersion = Persistence.feedVersions.getById(transformation.sourceVersionId);
                if (sourceVersion == null) {
                    status.fail("Source version ID must reference valid version.");
                    return;
                }
                if (transformation.table == null) {
                    status.fail("Must specify transformation table name.");
                    return;
                }
                String tableName = transformation.table + ".txt";
                String tableNamePath = "/" + tableName;

                // Run the replace transformation
                Path sourceZipPath = Paths.get(sourceVersion.retrieveGtfsFile().getAbsolutePath());
                try (FileSystem sourceZipFs = FileSystems.newFileSystem(sourceZipPath, null)) {
                    // If the source txt file does not exist, NoSuchFileException will be thrown and caught below.
                    Path sourceTxtFilePath = sourceZipFs.getPath(tableNamePath);
                    Path targetZipPath = Paths.get(target.retrieveGtfsFile().getAbsolutePath());
                    try( FileSystem targetZipFs = FileSystems.newFileSystem(targetZipPath, null) ){
                        Path targetTxtFilePath = targetZipFs.getPath(tableNamePath);
                        // Copy a file into the zip file, replacing it if it already exists.
                        Files.copy(sourceTxtFilePath, targetTxtFilePath, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (NoSuchFileException e) {
                    status.fail("Source version does not contain table: " + tableName, e);
                } catch (Exception e) {
                    status.fail("Unknown error encountered while transforming zip file", e);
                }
                System.out.println(target.retrieveGtfsFile().getAbsolutePath());
                return;
            }
            default: {
                // FIXME
                status.fail(String.format("Transform type %s not currently supported!", transformation.transformType));
            }
        }
    }
}
