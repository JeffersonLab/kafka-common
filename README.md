# kafka-common [![Java CI with Gradle](https://github.com/JeffersonLab/kafka-common/actions/workflows/ci.yml/badge.svg)](https://github.com/JeffersonLab/kafka-common/actions/workflows/ci.yml) [![Maven Central](https://badgen.net/maven/v/maven-central/org.jlab/kafka-common)](https://repo1.maven.org/maven2/org/jlab/kafka-common/)

Common Java utilities for Apache Kafka.  Currently the library provides support for [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) [[1](https://www.confluent.io/blog/okay-store-data-apache-kafka/)], [[2](https://www.confluent.io/blog/publishing-apache-kafka-new-york-times/)], [[3](https://www.confluent.io/blog/event-sourcing-cqrs-stream-processing-apache-kafka-whats-connection/)] and Json Serde. 

---
- [Overview](https://github.com/JeffersonLab/kafka-common#overview)
- [Install](https://github.com/JeffersonLab/kafka-common#install)
- [API](https://github.com/JeffersonLab/kafka-common#api)
- [Configure](https://github.com/JeffersonLab/kafka-common#configure)
- [Build](https://github.com/JeffersonLab/kafka-common#build)
- [Test](https://github.com/JeffersonLab/kafka-common#test)
- [See Also](https://github.com/JeffersonLab/kafka-common#see-also)
---

## Overview
This librray supports treating Kafka as a persistent database, and we casually refer to this as Event Sourcing.   We're using the term Event Sourcing to mean a real-time feed of changes, that can also be replayed to build the full state for anyone (re)joining mid-stream.  This is accomplished in Kafka with log compacted topics. We are using an Event Sourcing scheme with a simple entire record (database row / entity) per message approach.  All fields for a given record are always present (or implied null). The entire state of a given entity is always stored in a single message, the message key is required to be non-null and acts as the "primary key" in relational database terms, and the message value is the rest of the entity state (some fields may still act as a 'foreign key' reference though). 

We could have instead allowed a more granular scheme where a message only contains a single field that is changing (key would then need to include field name in order for compaction to play nicely), and that would have at least two benefits: 
  1. Clients would not need to know the entire state of a record when making single field changes (currently clients must re-set even the fields they aren't intending to change in the record)
  1. Save on single message size with single field changes (good with large records with a single field that changes frequently) 
 
We went with the atomic strategy since it is easier to deal with - clients don't have to merge records, they're always replace and our use-cases so far haven't required stateless producers or small individual messages.  Plus aggregate record size is smaller since no need for a composite key (field + record ID).   To make an update simply produce a new message containing the full state of the record.  To delete a record, produce a [tombstone](https://kafka.apache.org/documentation.html#compaction) message (null).

### Why not use Consumer API or Kafka Streams KTable API?
This library is somewhere in-between the Kafka [Consumer API](https://kafka.apache.org/documentation/#consumerapi) and the Kafka [Streams KTable API](https://kafka.apache.org/32/documentation/streams/).  You could just use the regular Consumer API just fine, but you'd likely end-up duplicating some code in each of your clients that can be refactored into a shared library - that's where this lib came from.   The Kafka Streams KTable API is overkill for many simple apps such as a command line utilty that just reads all records, dumps them to the console, and quits.    Other use-cases where Kafka Streams KTable is overkill include the [epics2kafka](https://github.com/JeffersonLab/epics2kafka) app, which consumes from a "command" topic: a relatively small, single partion, unreplicated topic that instructs the app what to do, and the app is already a Kafka Connect app so embedding a Kafka Streams app inside would add awkward complexity.  Finally, the [jaws-admin-gui](https://github.com/JeffersonLab/jaws-admin-gui) proxies Kakfa topics to web browser clients via Server Sent Events, and in this case it's a one-to-one connection that wouldn't benefit from the Kafka Streams infrastructure, but would certainly be complicated by it.

## Install

This library requires a Java 11+ JVM and standard library at run time.

You can obtain the library jar file from the [Maven Central repository](https://repo1.maven.org/maven2/org/jlab/kafka-common) directly or from a Maven friendly build tool with the following coordinates (Gradle example shown):
```
implementation 'org.jlab:kafka-common:<version>'
```
Check the [Release Notes](https://github.com/JeffersonLab/kafka-common/releases) to see what has changed in each version.

## API
[Javadocs](https://jeffersonlab.github.io/kafka-common)

## Configure
The EventSourceTable class (simplier version of KTable with some similarities to a standard Kafka Consumer class) is configured with the [EventSourceConfig](https://github.com/JeffersonLab/kafka-common/blob/main/src/main/java/org/jlab/kafka/eventsource/EventSourceConfig.java) class, which extends the common Kafka AbstractConfig.

## Build
This project is built with [Java 17](https://adoptium.net/) (compiled to Java 11 bytecode), and uses the [Gradle 7](https://gradle.org/) build tool to automatically download dependencies and build the project from source:

```
git clone https://github.com/JeffersonLab/kafka-common
cd kafka-common
gradlew build
```
**Note**: If you do not already have Gradle installed, it will be installed automatically by the wrapper script included in the source

**Note for JLab On-Site Users**: Jefferson Lab has an intercepting [proxy](https://gist.github.com/slominskir/92c25a033db93a90184a5994e71d0b78)

## Test
Continuous Integration (CI) is setup using GitHub Actions, so on push tests are automatically run unless [no ci] is included in the commit message. Tests can be manually run on a local workstation using:
```
docker compose -f test.yml up
```

Wait for containers to start then:
```
gradlew integrationTest
```

**Note**: By default integration tests require localhost port 9094 be available for Kafka.

## See Also
- [jaws-libj](https://github.com/JeffersonLab/jaws-libj)
- [epics2kafka](https://github.com/JeffersonLab/epics2kafka)
