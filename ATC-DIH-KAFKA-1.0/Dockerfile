FROM openjdk:8
RUN addgroup --system --gid 1001 blobtest
RUN adduser --system --uid  1001 --group blobtest
RUN chown -R blobtest:blobtest /opt
USER blobtest
WORKDIR .
COPY target/ATC-DIH-KAFKA-0.0.1-SNAPSHOT.jar .

