package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.jobs.CreateSnapshotJob;
import com.conveyal.datatools.editor.jobs.ExportSnapshotToGTFSJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0Connection;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.CreateFeedVersionFromSnapshotJob;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.JobUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import spark.HaltException;
import spark.Request;
import spark.Response;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.conveyal.datatools.TestUtils.assertThatSqlCountQueryYieldsExpectedCount;
import static com.conveyal.datatools.TestUtils.createFeedSource;
import static com.conveyal.datatools.TestUtils.createFeedVersion;
import static com.conveyal.datatools.TestUtils.createProject;
import static com.conveyal.datatools.TestUtils.isRunningE2E;
import static com.conveyal.datatools.TestUtils.zipFolderFiles;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class StatusControllerCancellationTest {
    private static final Auth0UserProfile USER = new Auth0UserProfile("owner@example.com", "owner");
    private Executor previousExecutor;
    private Map<String, Set<MonitorableJob>> previousJobs;
    private ExecutorService worker;
    private boolean previousAuthDisabled;

    @BeforeEach
    void setUp() throws IOException {
        assumeFalse(isRunningE2E(), "This test uses the local test server and databases.");
        DatatoolsTest.setUp();
        previousAuthDisabled = Auth0Connection.isAuthDisabled();
        Auth0Connection.setAuthDisabled(true);
        previousExecutor = JobUtils.heavyExecutor;
        previousJobs = JobUtils.userJobsMap;
        JobUtils.userJobsMap = new ConcurrentHashMap<>();
        worker = Executors.newSingleThreadExecutor();
        // Override the heavy executor to use a single-threaded executor so that we can control the execution of jobs in the test.
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
            Auth0Connection.setAuthDisabled(previousAuthDisabled);
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
        awaitWorker(10);

        assertTrue(job.status.cancelled);
        assertTrue(job.rolledBack.get(), "Cancellation should roll back the job.");
    }

    @Test
    void cancelSnapshotCreatedThroughApi() throws Exception {
        Auth0UserProfile user = Auth0Connection.getDefaultTestUser();
        Project project = createProject("Snapshot cancellation test");
        try {
            FeedSource feedSource = createFeedSource("Snapshot cancellation feed", project);
            FeedVersion feedVersion = createFeedVersion(
                feedSource, zipFolderFiles("fake-agency-with-only-calendar")
            );
            assertNotNull(feedVersion.namespace);
            assertNotNull(Persistence.feedVersions.getById(feedVersion.id));
            assertThatSqlCountQueryYieldsExpectedCount(
                "SELECT count(*) FROM " + feedVersion.namespace + ".stops", 5
            );

            Snapshot buffer = new Snapshot("Editor buffer", feedSource.id, feedVersion.namespace);
            CreateSnapshotJob importJob = new CreateSnapshotJob(user, buffer, true, false, false);
            importJob.run();
            assertTrue(importJob.status.completed);
            assertFalse(importJob.status.error, importJob.status.exceptionDetails);
            String originalBuffer = Persistence.feedSources.getById(feedSource.id).editorNamespace;
            assertNotNull(originalBuffer);
            assertTrue(feedSource.retrieveSnapshots().isEmpty());

            assertFalse(DataManager.useS3, "This test verifies rollback of local export files.");
            FeedStore previousFeedStore = FeedVersion.feedStore;
            FeedStore exportStore = spy(previousFeedStore);
            CountDownLatch exported = new CountDownLatch(1);
            CountDownLatch releaseExport = new CountDownLatch(1);
            AtomicReference<File> exportedFile = new AtomicReference<>();
            Set<Path> previousTempFiles = snapshotTempFiles();

            // Keep the export active until cancellation is requested, so the test exercises rollback of an in-progress export.
            doAnswer(invocation -> {
                File file = (File) invocation.callRealMethod();
                exportedFile.set(file);
                exported.countDown();
                if (!releaseExport.await(120, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting for cancellation of the export.");
                }
                return file;
            }).when(exportStore).newFeed(anyString(), any(InputStream.class), isNull());
            FeedVersion.feedStore = exportStore;

            String snapshotJobId;
            try {
                // Create a snapshot through the API, which will trigger the export to GTFS and then cancel it.
                snapshotJobId = given()
                    .port(DataManager.PORT)
                    .queryParam("feedId", feedSource.id)
                    .queryParam("publishNewVersion", true)
                    .contentType("application/json")
                    .body("{\"name\":\"Snapshot to cancel\"}")
                    .post("/api/editor/secure/snapshot")
                    .then().statusCode(200)
                    .extract().jsonPath().getString("jobId");
                assertTrue(exported.await(120, TimeUnit.SECONDS), "Snapshot was not exported to a GTFS file.");

                // Verify that the snapshot job and its child jobs are active and have created the expected data before cancellation.
                MonitorableJob rootJob = JobUtils.getJobById(user.getUser_id(), snapshotJobId, false);
                assertNotNull(rootJob);
                assertEquals(1, rootJob.subJobs.size());
                assertTrue(rootJob.subJobs.get(0) instanceof CreateFeedVersionFromSnapshotJob);

                MonitorableJob createFeedVersionFromSnapshotJob = rootJob.subJobs.get(0);
                assertTrue(
                    createFeedVersionFromSnapshotJob.active,
                    "CreateFeedVersionFromSnapshotJob must have started before cancellation."
                );

                assertTrue(createFeedVersionFromSnapshotJob.subJobs.get(0) instanceof ExportSnapshotToGTFSJob);
                ExportSnapshotToGTFSJob exportSnapshotToGTFSJob = (ExportSnapshotToGTFSJob) createFeedVersionFromSnapshotJob.subJobs.get(0);
                assertTrue(exportSnapshotToGTFSJob.active, "ExportSnapshotToGTFSJob must have started before cancellation.");

                Snapshot snapshot = exportSnapshotToGTFSJob.getSnapshot();
                assertNotNull(snapshot.namespace);
                assertThatSqlCountQueryYieldsExpectedCount(
                    "SELECT count(*) FROM " + snapshot.namespace + ".stops", 5
                );
                assertTrue(exportedFile.get().isFile(), "ExportSnapshotToGTFSJob must create a real file before cancellation.");
                Set<Path> exportTempFiles = snapshotTempFiles();
                exportTempFiles.removeAll(previousTempFiles);
                assertEquals(1, exportTempFiles.size(), "ExportSnapshotToGTFSJob should have created a temporary GTFS zip.");

                // Cancel the snapshot job through the API, which should cancel both child jobs and roll back their work.
                given()
                    .port(DataManager.PORT)
                    .delete("/api/manager/secure/status/jobs/" + snapshotJobId)
                    .then().statusCode(202)
                    .body("jobId", equalTo(snapshotJobId))
                    .body("status.cancellationRequested", equalTo(true));
                assertFalse(rootJob.status.cancelled, "Rollback must wait for the export to return.");
                releaseExport.countDown();

                // Wait for the export to finish rolling back, which should also roll back the feed version creation.
                awaitWorker(120);

                // Verify that the snapshot job and its child jobs are canceled and rolled back, and that the
                // exported file and temporary files are removed.
                assertTrue(createFeedVersionFromSnapshotJob.status.cancelled, createFeedVersionFromSnapshotJob.status.exceptionDetails);
                assertNull(createFeedVersionFromSnapshotJob.status.exceptionDetails);
                assertTrue(exportSnapshotToGTFSJob.status.cancelled, exportSnapshotToGTFSJob.status.exceptionDetails);
                assertNull(exportSnapshotToGTFSJob.status.exceptionDetails);
                assertFalse(exportedFile.get().exists(), "CreateFeedVersionFromSnapshotJob rollback must remove the exported feed.");
                verify(exportStore).deleteFeed(exportedFile.get().getName());
                for (Path tempFile : exportTempFiles) {
                    assertFalse(Files.exists(tempFile), "ExportSnapshotToGTFSJob rollback must remove its temporary GTFS zip.");
                }
                assertNull(Persistence.feedVersions.getById(exportedFile.get().getName()));
                assertEquals(1, feedSource.retrieveFeedVersions().size());
                assertNull(Persistence.snapshots.getById(snapshot.id));
                assertThatSqlCountQueryYieldsExpectedCount(
                    "SELECT count(*) FROM information_schema.schemata WHERE schema_name = '" + snapshot.namespace + "'", 0
                );
            } finally {
                releaseExport.countDown();
                try {
                    awaitWorker(120);
                } finally {
                    FeedVersion.feedStore = previousFeedStore;
                }
            }

            // Wait for the snapshot job to finish rolling back, which should also roll back the feed version creation.
            awaitWorker(120);
            MonitorableJob cancelled = JobUtils.getJobById(user.getUser_id(), snapshotJobId, false);
            assertNotNull(cancelled);
            assertTrue(cancelled.status.completed);
            assertTrue(cancelled.status.cancelled, cancelled.status.exceptionDetails);
            assertFalse(cancelled.active);
            assertNull(cancelled.status.exceptionDetails);
            assertTrue(feedSource.retrieveSnapshots().isEmpty(), "The cancelled snapshot must not be stored.");
            assertEquals(originalBuffer, Persistence.feedSources.getById(feedSource.id).editorNamespace);
            assertEquals(feedVersion.namespace, Persistence.feedVersions.getById(feedVersion.id).namespace);
            assertThatSqlCountQueryYieldsExpectedCount(
                "SELECT count(*) FROM " + feedVersion.namespace + ".stops", 5
            );
            assertThatSqlCountQueryYieldsExpectedCount(
                "SELECT count(*) FROM " + originalBuffer + ".stops", 5
            );
        } finally {
            awaitWorker(120);
            project.delete();
        }
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
        awaitWorker(10);
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
        awaitWorker(10);

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

    /**
     * Wait until everything submitted before this point has finished, but give up waiting after the specified number of seconds.
     */
    private void awaitWorker(int secondsToWait) throws Exception {
        worker.submit(() -> { }).get(secondsToWait, TimeUnit.SECONDS);
    }

    private Set<Path> snapshotTempFiles() throws IOException {
        try (Stream<Path> files = Files.list(new File(System.getProperty("java.io.tmpdir")).toPath())) {
            return files.filter(path -> {
                String name = path.getFileName().toString();
                return name.startsWith("snapshot") && name.endsWith("zip");
            }).collect(Collectors.toSet());
        }
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
