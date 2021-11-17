package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.loader.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.manager.jobs.feedmerge.MergeFeedsType.SERVICE_PERIOD;

public class AgencyMergeLineContext extends MergeLineContext {
    private static final Logger LOG = LoggerFactory.getLogger(AgencyMergeLineContext.class);

    public AgencyMergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        super(job, table, out);
    }

    @Override
    public void checkFirstLineConditions() {
        checkForMissingAgencyId();
    }

    @Override
    public boolean shouldProcessRows() {
        return !checkMismatchedAgency();
    }

    private void checkForMissingAgencyId() {
        if ((keyFieldMissing || keyValue.equals(""))) {
            // agency_id is optional if only one agency is present, but that will
            // cause issues for the feed merge, so we need to insert an agency_id
            // for the single entry.
            (isHandlingActiveFeed()
                ? feedMergeContext.active
                : feedMergeContext.future
            ).setNewAgencyId(UUID.randomUUID().toString());

            if (keyFieldMissing) {
                // Only add agency_id field if it is missing in table.
                addField(Table.AGENCY.fields[0]);
            }
        }
    }

    /**
     * Check for some conditions that could occur when handling a service period merge.
     *
     * @return true if the merge encountered failing conditions
     */
    private boolean checkMismatchedAgency() {
        if (isHandlingActiveFeed() && job.mergeType.equals(SERVICE_PERIOD)) {
            // If merging the agency table, we should only skip the following feeds if performing an MTC merge
            // because that logic assumes the two feeds share the same agency (or
            // agencies). NOTE: feed_info file is skipped by default (outside of this
            // method) for a regional merge), which is why this block is exclusively
            // for an MTC merge. Note, this statement may print multiple log
            // statements, but it is deliberately nested in the csv while block in
            // order to detect agency_id mismatches and fail the merge if found.
            // The second feed's agency table must contain the same agency_id
            // value as the first feed.
            String agencyId = String.join(":", keyField, keyValue);
            if (!"".equals(keyValue) && !referenceTracker.transitIds.contains(agencyId)) {
                String otherAgencyId = referenceTracker.transitIds.stream()
                    .filter(transitId -> transitId.startsWith(AGENCY_ID))
                    .findAny()
                    .orElse(null);
                job.failMergeJob(String.format(
                    "MTC merge detected mismatching agency_id values between two "
                        + "feeds (%s and %s). Failing merge operation.",
                    agencyId,
                    otherAgencyId
                ));
                return true;
            }
            LOG.warn("Skipping {} file for feed {}/{} (future file preferred)", table.name, getFeedIndex(), feedMergeContext.feedsToMerge.size());
            skipFile = true;
        }
        return false;
    }
}