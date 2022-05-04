# kafka-common [![Java CI with Gradle](https://github.com/JeffersonLab/kafka-common/actions/workflows/gradle.yml/badge.svg)](https://github.com/JeffersonLab/kafka-common/actions/workflows/gradle.yml) [![Maven Central](https://badgen.net/maven/v/maven-central/org.jlab/kafka-common)](https://repo1.maven.org/maven2/org/jlab/kafka-common/)
Common Java utilities for Apache Kafka
---
- [Install](https://github.com/JeffersonLab/kafka-common#install)
- [API](https://github.com/JeffersonLab/kafka-common#api)
- [Configure](https://github.com/JeffersonLab/kafka-common#configure)
- [Build](https://github.com/JeffersonLab/kafka-common#build)
---

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
