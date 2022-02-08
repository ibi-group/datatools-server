package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.Table;
import com.csvreader.CsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType.SERVICE_PERIOD;
import static com.conveyal.datatools.manager.utils.MergeFeedUtils.stopCodeFailureMessage;

public class StopsMergeLineContext extends MergeLineContext {
    private static final Logger LOG = LoggerFactory.getLogger(StopsMergeLineContext.class);

    private boolean stopCodeMissingFromFutureFeed = false;

    public StopsMergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        super(job, table, out);
    }

    @Override
    public void checkFirstLineConditions() throws IOException {
        checkThatStopCodesArePopulatedWhereRequired();
    }

    @Override
    public boolean checkFieldsForMergeConflicts(Set<NewGTFSError> idErrors, FieldContext fieldContext) throws IOException {
        return checkRoutesAndStopsIds(idErrors, fieldContext);
    }

    @Override
    public void checkFieldsForReferences(FieldContext fieldContext) {
        updateParentStationReference(fieldContext);
    }

    /**
     * If there is a parent station reference, update to include the scope stop_id.
     */
    private void updateParentStationReference(FieldContext fieldContext) {
        if (fieldContext.nameEquals("parent_station")) {
            String parentStation = fieldContext.getValue();
            if (!"".equals(parentStation)) {
                LOG.debug("Updating parent station to: {}", getIdWithScope(parentStation));
                updateAndRemapOutput(fieldContext);
            }
        }
    }

    /**
     * Checks that the stop_code field of the Stop entities to merge is populated where required.
     * @throws IOException
     */
    private void checkThatStopCodesArePopulatedWhereRequired() throws IOException {
        if (shouldCheckStopCodes()) {
            // Before reading any lines in stops.txt, first determine whether all records contain
            // properly filled stop_codes. The rules governing this logic are as follows:
            // 1. Stops with location_type greater than 0 (i.e., anything but 0 or empty) are permitted
            //    to have empty stop_codes (even if there are other stops in the feed that have
            //    stop_code values). This is because these location_types represent special entries
            //    that are either stations, entrances/exits, or generic nodes (e.g., for
            //    pathways.txt).
            // 2. For regular stops (location_type = 0 or empty), all or none of the stops must
            //    contain stop_codes. Otherwise, the merge feeds job will be failed.
            int stopsMissingStopCodeCount = 0;
            int stopsCount = 0;
            int specialStopsCount = 0;
            int locationTypeIndex = getFieldIndex("location_type");
            int stopCodeIndex = getFieldIndex("stop_code");
            // Get special stops reader to iterate over every stop and determine if stop_code values
            // are present.
            CsvReader stopsReader = table.getCsvReader(feed.zipFile, null);
            while (stopsReader.readRecord()) {
                stopsCount++;
                // Special stop records (i.e., a station, entrance, or anything with
                // location_type > 0) do not need to specify stop_code. Other stops should.
                String stopCode = stopsReader.get(stopCodeIndex);
                boolean stopCodeIsMissing = "".equals(stopCode);
                String locationType = stopsReader.get(locationTypeIndex);
                if (isSpecialStop(locationType)) specialStopsCount++;
                else if (stopCodeIsMissing) stopsMissingStopCodeCount++;
            }
            stopsReader.close();
            LOG.info("total stops: {}", stopsCount);
            LOG.info("stops missing stop_code: {}", stopsMissingStopCodeCount);
            if (stopsMissingStopCodeCount + specialStopsCount == stopsCount) {
                // If all stops are missing stop_code (taking into account the special stops that do
                // not require stop_code), we simply default to merging on stop_id.
                LOG.warn(
                    "stop_code is not present in file {}/{}. Reverting to stop_id",
                    getFeedIndex() + 1, feedMergeContext.feedsToMerge.size());
                // If the key value for stop_code is not present, revert to stop_id.
                keyField = table.getKeyFieldName();
                keyFieldIndex = getKeyFieldIndex();
                keyValue = getCsvReader().get(keyFieldIndex);
                // When all stops missing stop_code for the first feed, there's nothing to do (i.e.,
                // no failure condition has been triggered yet). Just indicate this in the flag and
                // proceed with the merge.
                if (isHandlingFutureFeed()) {
                    stopCodeMissingFromFutureFeed = true;
                } else if (!stopCodeMissingFromFutureFeed) {
                    // However... if the second feed was missing stop_codes and the first feed was not,
                    // fail the merge job.
                    job.failMergeJob(
                        stopCodeFailureMessage(stopsMissingStopCodeCount, stopsCount, specialStopsCount)
                    );
                }
            } else if (stopsMissingStopCodeCount > 0) {
                // If some, but not all, stops are missing stop_code, the merge feeds job must fail.
                job.failMergeJob(
                    stopCodeFailureMessage(stopsMissingStopCodeCount, stopsCount, specialStopsCount)
                );
            }
        }
    }

    private boolean shouldCheckStopCodes() {
        return job.mergeType.equals(SERVICE_PERIOD);
    }

    /** Determine if stop is "special" via its locationType. I.e., a station, entrance, (location_type > 0). */
    private boolean isSpecialStop(String locationType) {
        return !"".equals(locationType) && !"0".equals(locationType);
    }
}