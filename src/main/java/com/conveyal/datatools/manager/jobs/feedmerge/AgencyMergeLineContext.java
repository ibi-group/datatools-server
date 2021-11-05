package com.conveyal.datatools.manager.jobs.feedmerge;

import com.conveyal.datatools.manager.jobs.MergeFeedsJob;
import com.conveyal.gtfs.loader.Field;
import com.conveyal.gtfs.loader.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipOutputStream;

public class AgencyMergeLineContext extends MergeLineContext {
    public AgencyMergeLineContext(MergeFeedsJob job, Table table, ZipOutputStream out) throws IOException {
        super(job, table, out);
    }

    @Override
    public void checkFirstLineConditions() {
        checkForMissingAgencyId();
    }

    private void checkForMissingAgencyId() {
        if ((keyFieldMissing || keyValue.equals(""))) {
            // agency_id is optional if only one agency is present, but that will
            // cause issues for the feed merge, so we need to insert an agency_id
            // for the single entry.
            newAgencyId = UUID.randomUUID().toString();
            if (keyFieldMissing) {
                // Only add agency_id field if it is missing in table.
                List<Field> fieldsList = new ArrayList<>(Arrays.asList(fieldsFoundInZip));
                fieldsList.add(Table.AGENCY.fields[0]);
                fieldsFoundInZip = fieldsList.toArray(fieldsFoundInZip);
                allFields.add(Table.AGENCY.fields[0]);
            }
            fieldsFoundList = Arrays.asList(fieldsFoundInZip);
        }
    }
}