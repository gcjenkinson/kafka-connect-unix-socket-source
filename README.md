# Kafka Connect Unix Socket Source
The connector is used to write data to Kafka from a Unix Socket.

# Building
You can build the connector with Maven using the standard lifecycle phases:
```
mvn clean
mvn package
```

# Sample Configuration
``` ini
name=unix-socket-connector
connector.class=uk.cl.cam.ac.uk.cadets.UnixSocketSourceConnector
tasks.max=1
topics=topic_name
schema.name=socketschema
pathname=socket_name
batch.size=1
```
