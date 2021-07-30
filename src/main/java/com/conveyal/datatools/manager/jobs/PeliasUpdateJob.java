package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;

public class PeliasUpdateJob extends MonitorableJob {
    public PeliasUpdateJob(Auth0UserProfile owner, String name) {
        super(owner, name, JobType.UPDATE_PELIAS);
    }
    /**
     * This method must be overridden by subclasses to perform the core steps of the job.
     */
    @Override
    public void jobLogic() throws Exception {
        status.update("Here we go!", 5.0);
        Thread.sleep(4000);
        status.completeSuccessfully("it's all done :)");
    }
}
