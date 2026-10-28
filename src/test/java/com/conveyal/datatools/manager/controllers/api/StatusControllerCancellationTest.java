package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.utils.JobUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import spark.HaltException;
import spark.Request;
import spark.Response;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class StatusControllerCancellationTest {
    private static final Auth0UserProfile USER = new Auth0UserProfile("owner@example.com", "owner");
    private Executor previousExecutor;
    private Map<String, Set<MonitorableJob>> previousJobs;
    private ExecutorService worker;

    @BeforeEach
    void setUp() {
        previousExecutor = JobUtils.heavyExecutor;
        previousJobs = JobUtils.userJobsMap;
        JobUtils.userJobsMap = new ConcurrentHashMap<>();
        worker = Executors.newSingleThreadExecutor();
        JobUtils.heavyExecutor = worker;
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        worker.shutdownNow();
        try {
            assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS), "Job worker did not stop.");
        } finally {
            JobUtils.heavyExecutor = previousExecutor;
            JobUtils.userJobsMap = previousJobs;
        }
    }

    @Test
    void cancelsRunningJobAfterItsCurrentWorkFinishes() throws Exception {
        TestJob job = TestJob.blocking(USER);
        JobUtils.cancellableExecutor.execute(job);
        assertTrue(job.started.await(10, TimeUnit.SECONDS), "Job did not start.");

        Response response = mock(Response.class);
        assertSame(job, StatusController.cancelJob(request(job.jobId, USER), response));
        verify(response).status(202);
        assertTrue(job.status.cancellationRequested);
        assertFalse(job.status.cancelled, "The running work must finish before cleanup.");

        job.release.countDown();
        awaitWorker();

        assertTrue(job.status.cancelled);
        assertTrue(job.rolledBack.get(), "Cancellation should roll back the job.");
    }

    @Test
    void rejectsCancellationFromAnotherUser() throws Exception {
        TestJob job = TestJob.blocking(USER);
        JobUtils.cancellableExecutor.execute(job);
        assertTrue(job.started.await(10, TimeUnit.SECONDS), "Job did not start.");

        HaltException error = assertThrows(HaltException.class, () ->
            StatusController.cancelJob(
                request(job.jobId, new Auth0UserProfile("other@example.com", "other")),
                mock(Response.class)
            )
        );

        assertEquals(404, error.statusCode());
        assertFalse(job.status.cancellationRequested);
        job.release.countDown();
        awaitWorker();
        assertTrue(job.status.completed);
        assertFalse(job.status.error);
    }

    @Test
    void returnsNotFoundForUnknownJob() {
        HaltException missing = assertThrows(HaltException.class, () ->
            StatusController.cancelJob(request("missing", USER), mock(Response.class))
        );
        assertEquals(404, missing.statusCode());
    }

    @Test
    void rejectsCancellationForJobThatCannotBeCancelled() {
        TestJob unsupported = TestJob.nonCancellable(USER);
        HaltException conflict = assertThrows(HaltException.class, () ->
            StatusController.cancelJob(request(unsupported.jobId, USER), mock(Response.class))
        );
        assertEquals(409, conflict.statusCode());
    }

    @Test
    void rejectsCancellationAfterJobHasFinished() throws Exception {
        TestJob job = TestJob.finished(USER);
        JobUtils.cancellableExecutor.execute(job);
        awaitWorker();

        assertTrue(job.status.completed);
        assertFalse(job.status.error);
        HaltException conflict = assertThrows(HaltException.class, () ->
            StatusController.cancelJob(request(job.jobId, USER), mock(Response.class))
        );
        assertEquals(409, conflict.statusCode());
        assertFalse(job.status.cancellationRequested);
    }

    private Request request(String jobId, Auth0UserProfile user) {
        Request request = mock(Request.class);
        when(request.attribute("user")).thenReturn(user);
        when(request.params("jobId")).thenReturn(jobId);
        when(request.pathInfo()).thenReturn("/api/manager/secure/status/jobs/" + jobId);
        return request;
    }

    private void awaitWorker() throws Exception {
        worker.submit(() -> { }).get(10, TimeUnit.SECONDS);
    }

    private static class TestJob extends MonitorableJob {
        private final boolean cancellable;
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicBoolean rolledBack = new AtomicBoolean();
        private final boolean waitForRelease;

        private TestJob(Auth0UserProfile user, boolean cancellable, boolean waitForRelease) {
            super(user);
            this.cancellable = cancellable;
            this.waitForRelease = waitForRelease;
        }

        private static TestJob blocking(Auth0UserProfile user) {
            return new TestJob(user, true, true);
        }

        private static TestJob nonCancellable(Auth0UserProfile user) {
            return new TestJob(user, false, false);
        }

        private static TestJob finished(Auth0UserProfile user) {
            return new TestJob(user, true, false);
        }

        @Override
        public boolean supportsCancellation() {
            return cancellable;
        }

        @Override
        public void jobLogic() {
            started.countDown();
            if (waitForRelease) {
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out waiting for the test to release the job.");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for the test.", e);
                }
            }
        }

        @Override
        protected void rollbackCancellation() {
            rolledBack.set(true);
        }
    }
}
