package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.Deployment;
import com.conveyal.datatools.manager.models.Deployment.SummarizedFeedVersion;

import java.io.IOException;
import java.nio.file.Files;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/** Export routes or stops for a GTFS feed version as a shapefile. */
public class DeploymentGisExportJob extends GisExportJob {
    public List<SummarizedFeedVersion> feedVersions;
    public Deployment deployment;

    public DeploymentGisExportJob(GisExportJob.ExportType exportType, Deployment deployment, File file, Auth0UserProfile owner) {
        super(exportType, file, deployment.feedVersionIds, owner);
        this.deployment = deployment;
        this.feedVersions = deployment.retrieveFeedVersions();
    }

    @Override public void jobLogic() {
        LOG.info("Storing shapefiles for deployment {}", deployment.name);

        try {
            File outDir = setupGisExport();

            for (SummarizedFeedVersion feedVersion: feedVersions) {
                //TODO: update progress bar after each feedVersion?
                Path outputPath = Paths.get(outDir.getPath() + "/" + feedVersion.feedSource.name);
                File feedVersionFolder = Files.createDirectory(outputPath).toFile();
                // feedId needs to be a collection for the base GisExportJob packageShapefiles method.
                feedIds = Collections.singletonList(feedVersion.id);

                // Pass feedVersionFolder as the outDir to write output to a feed-specific folder.
                packageShapefiles(feedVersionFolder, feedVersion.feedSource.name + ".shp" );
            }
            zipShapefiles(outDir);
        }
        catch (IOException e) {
            status.fail("An IOException occurred while exporting the GIS shapefiles", e);
        }
    }
}

