package com.conveyal.datatools.editor.jobs;

import play.jobs.Job;

public class ProcessGtfsSnapshotUpload extends Job {
/*
    private Long _gtfsSnapshotMergeId;

    private Map<String, BigInteger> agencyIdMap = new HashMap<String, BigInteger>();

    public ProcessGtfsSnapshotUpload(Long gtfsSnapshotMergeId) {
        this._gtfsSnapshotMergeId = gtfsSnapshotMergeId;
    }

    public void doJob() {

        GtfsSnapshotMerge snapshotMerge = null;
        while(snapshotMerge == null)
        {
            snapshotMerge = GtfsSnapshotMerge.findById(this._gtfsSnapshotMergeId);
            LOG.warn("Waiting for snapshotMerge to save...");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        GtfsReader reader = new GtfsReader();
        GtfsDaoImpl store = new GtfsDaoImpl();

        Long agencyCount = new Long(0);

        try {

            File gtfsFile = new File(Play.configuration.getProperty("application.publicGtfsDataDirectory"), snapshotMerge.snapshot.getFilename());

            reader.setInputLocation(gtfsFile);
            reader.setEntityStore(store);
            reader.run();

            LOG.info("GtfsImporter: listing agencies...");

            for (org.onebusaway.gtfs.model.Agency gtfsAgency : reader.getAgencies()) {

                GtfsAgency agency = new GtfsAgency(gtfsAgency);
                agency.snapshot = snapshotMerge.snapshot;
                agency.save();

            }

            snapshotMerge.snapshot.agencyCount = store.getAllAgencies().size();
            snapshotMerge.snapshot.routeCount = store.getAllRoutes().size();
            snapshotMerge.snapshot.stopCount = store.getAllStops().size();
            snapshotMerge.snapshot.tripCount = store.getAllTrips().size();

            snapshotMerge.snapshot.save();

        }
        catch (Exception e) {

            LOG.error(e.toString());

            snapshotMerge.failed(e.toString());
        }
    }*/
}

