# syntax=docker/dockerfile:1
FROM openjdk:11
WORKDIR /datatools

# Grab latest dev build
COPY target/dt*.jar ./datatools-server-3.8.1-SNAPSHOT.jar

RUN mkdir -p /var/datatools_gtfs
# Launch server (relies on env.yml being placed in volume!)
# Try: docker run --publish 4000:4000 -v ~/config/:/config datatools-latest
CMD ["java", "-jar", "datatools-server-3.8.1-SNAPSHOT.jar", "/config/env.yml", "/config/server.yml"]
EXPOSE 4000