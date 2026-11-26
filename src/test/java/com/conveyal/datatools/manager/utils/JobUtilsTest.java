package com.conveyal.datatools.manager.utils;

import com.conveyal.datatools.common.status.CancellationContext;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;

class JobUtilsTest {

    @Test
    void testRetainsActiveJobsUntilCleanupHasFinished() {
        MonitorableJob job = createMonitorableJob();
        job.active = true;
        job.status.fail("Finishing cleanup after failure.");
        try {
            assertTrue(JobUtils.filterStagedAndActiveJobs(Sets.newHashSet(job)).contains(job));
            assertSame(job, JobUtils.getJobById(job.retrieveUserId(), job.jobId, true));
            assertTrue(JobUtils.getJobsByUserId(job.retrieveUserId(), false).contains(job));
        } finally {
            job.active = false;
            JobUtils.getJobsByUserId(job.retrieveUserId(), false).remove(job);
        }
    }

    @Test
    void testRetainsCompletedChildJobsUntilCancellationContextFinishes() throws Exception {
        CountDownLatch blockingChildStarted = new CountDownLatch(1);
        CountDownLatch finishBlockingChild = new CountDownLatch(1);
        MonitorableJob root = new MonitorableJob() {
            @Override
            public boolean supportsCancellation() {
                return true;
            }

            @Override
            public void jobLogic() {
            }
        };
        MonitorableJob singleLookupChild = createMonitorableJob();
        MonitorableJob bulkLookupChild = createMonitorableJob();
        MonitorableJob blockingChild = new MonitorableJob() {
            @Override
            public void jobLogic() throws Exception {
                blockingChildStarted.countDown();
                finishBlockingChild.await();
            }
        };
        root.addNextJob(singleLookupChild, bulkLookupChild, blockingChild);
        root.enableCancellation();
        Thread worker = new Thread(root);
        worker.start();

        try {
            assertTrue(blockingChildStarted.await(5, TimeUnit.SECONDS));
            assertFalse(singleLookupChild.active);
            assertFalse(bulkLookupChild.active);
            assertSame(singleLookupChild, JobUtils.getJobById(
                root.retrieveUserId(), singleLookupChild.jobId, true
            ));
            assertTrue(JobUtils.userJobsMap.get(root.retrieveUserId()).contains(singleLookupChild));

            Set<MonitorableJob> jobs = JobUtils.getJobsByUserId(root.retrieveUserId(), true);
            assertTrue(jobs.contains(bulkLookupChild));
            assertTrue(JobUtils.userJobsMap.get(root.retrieveUserId()).contains(bulkLookupChild));
            finishBlockingChild.countDown();
            worker.join(5000);

            assertFalse(worker.isAlive());
            assertSame(singleLookupChild, JobUtils.getJobById(
                root.retrieveUserId(), singleLookupChild.jobId, true
            ));
            assertFalse(JobUtils.userJobsMap.get(root.retrieveUserId()).contains(singleLookupChild));
            JobUtils.getJobsByUserId(root.retrieveUserId(), true);
            assertFalse(JobUtils.userJobsMap.get(root.retrieveUserId()).contains(bulkLookupChild));
        } finally {
            finishBlockingChild.countDown();
            worker.join(5000);
            Set<MonitorableJob> remainingJobs = JobUtils.getJobsByUserId(root.retrieveUserId(), false);
            remainingJobs.remove(root);
            remainingJobs.remove(singleLookupChild);
            remainingJobs.remove(bulkLookupChild);
            remainingJobs.remove(blockingChild);
        }
    }

    @Test
    void testRollbackAttemptsAllResourcesAndReportsFailures() {
        AtomicBoolean cleaned = new AtomicBoolean();
        Exception first = new Exception("First cleanup failed");
        Exception second = new Exception("Second cleanup failed");

        Exception failure = assertThrows(Exception.class, () -> CancellationContext.rollbackResources(
            () -> { throw first; },
            () -> { throw second; },
            () -> cleaned.set(true)
        ));

        assertTrue(cleaned.get());
        assertEquals(first, failure);
        assertEquals(second, failure.getSuppressed()[0]);
    }

    @Test
    void testRemoveJobsOlderThanAWeek() {
        JobUtils.userJobsMap.clear();
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