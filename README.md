# Transit Data Manager

[![Join the chat at https://matrix.to/#/#transit-data-tools:gitter.im](https://badges.gitter.im/repo.png)](https://matrix.to/#/#transit-data-tools:gitter.im)

The core application for IBI Group's TRANSIT-Data-Tools suite.

## Documentation

View the [latest documentation](http://conveyal-data-tools.readthedocs.org/en/latest/) at ReadTheDocs.

Note: `dev` branch docs can be found [here](http://conveyal-data-tools.readthedocs.org/en/dev/).

### Cancelling snapshot jobs

Send `DELETE /api/manager/secure/status/jobs/:jobId` as the job's owner to cancel a snapshot
creation, import, or restore, including its chained feed-version publication. The endpoint returns
`202` when cancellation is requested, `404` for an unknown job or another user's job, and `409`
for unsupported jobs or jobs whose cancellation window has closed.

Cancellation is cooperative: an active database operation finishes before the job stops and
rolls back its newly created schemas, snapshot records, and feed-version files. Publication,
notifications, shared S3 latest/public download updates, and automatic follow-up jobs are deferred
until the cancellation window closes.
Poll the job status: `cancellationRequested` acknowledges the request, while `cancelled` becomes
true only after cleanup succeeds. A cleanup failure is reported as a job error with exception
details, not a successful cancellation. Cancellation is no longer accepted during final commit.

## Docker Image
The easiest way to get `datatools-server` running is to use the provided `Dockerfile` and `docker-compose.yml`. The `docker-compose.yml` includes both database servers that are needed. Edit the supplied configurations in the `configurations` directory to ensure the server starts correctly. Once this is done running `docker-compose up` will start Datatools and all required database servers.

## Getting in touch

We have a Gitter [space](https://matrix.to/#/#transit-data-tools:gitter.im) for the full TRANSIT-Data-Tools project where you can post questions and comments.
