package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobUtilsTest {

    @BeforeEach
    void setUp() {
        JobUtils.userJobsMap.clear();
    }

    @Test
    void testRemoveJobsOlderThanAWeek() {
        String userId = "user-id";
        Set<MonitorableJob> jobs = Sets.newConcurrentHashSet();

        MonitorableJob oldJob = createMonitorableJob();
        oldJob.status.modified = Instant.now().minus(java.time.Duration.ofDays(8)).toString();

        MonitorableJob recentJob = createMonitorableJob();
        recentJob.status.modified = Instant.now().toString();

        jobs.add(oldJob);
        jobs.add(recentJob);
        JobUtils.userJobsMap.put(userId, jobs);

        JobUtils.removeJobsOlderThanAWeek();

        Set<MonitorableJob> remainingJobs = JobUtils.userJobsMap.get(userId);
        assertEquals(1, remainingJobs.size());
        assertTrue(remainingJobs.stream().anyMatch(job -> job.jobId.equals(recentJob.jobId)));
    }

    private MonitorableJob createMonitorableJob() {
        return new MonitorableJob() {
            @Override
            public void jobLogic() {

            }
        };
    }
}