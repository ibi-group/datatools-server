# Transit Data Manager

The core application for IBI Group's transit data tools suite.

## Documentation

View the [latest documentation](http://conveyal-data-tools.readthedocs.org/en/latest/) at ReadTheDocs.

Note: `dev` branch docs can be found [here](http://conveyal-data-tools.readthedocs.org/en/dev/).

## Docker Image
The easiest way to get `datatools-server` running is to use the provided `Dockerfile` and `docker-compose.yml`. The `docker-compose.yml` includes both database servers that are needed. Edit the supplied configurations in the `configurations` directory to ensure the server starts correctly. Once this is done running `docker-compose up` will start Datatools and all required database servers.