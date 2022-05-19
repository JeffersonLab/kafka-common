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
This librray supports treating Kafka as a persistent database, and we casually refer to this as Event Sourcing. The Event Sourcing scheme supported by this library is a simple entire record (database row / entity) per message approach.  All fields for a given record are always present (or implied null). The entire state of a given entity is always stored in a single message, the message key is required to be non-null and acts as the "primary key" in relational database terms, and the message value is the rest of the entity state (some fields may still act as a 'foreign key' reference though). 

We could have instead allowed a more granular scheme where a message only contains a single field that is changing (key would then need to include field name in order for compaction to play nicely), and that would have at least two benefits: 
  1. Save on message size with single field changes 
  2. Clients would not need to know the entire state of a record when making single field changes (currently clients must re-set even the fields they aren't intending to change in the record)
 
We went with the atomic strategy since it is easier to deal with - clients don't have to merge records, they're always replace and our use-cases so far haven't required space efficiency.   To make an update simply produce a new message containing the full state of the record.  To delete a record, produce a [tombstone](https://kafka.apache.org/documentation.html#compaction) message (null).



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
The EventSourceTable class (simplier version of KTable with some similarities to a standard Kafka Consumer class) is configured with the [EventSourceConfig](https://github.com/JeffersonLab/kafka-common/blob/main/src/main/java/org/jlab/kafka/eventsource/EventSourceConfig.java) class, which extends the common Kafka AbstractConfig.  Unlike the Kafka Streams _commit.interval.ms_ and _cache.max.byte.buffering_ configs EventSourceTable uses _event.source.poll.millis_ and _event.source.max.poll.records_.

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
