# step-framework

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE.txt)
[![Maven Central](https://img.shields.io/maven-central/v/ch.exense.step/step-framework)](https://central.sonatype.com/artifact/ch.exense.step/step-framework)

A modular Java framework providing reusable infrastructure components for building robust, production-grade applications. Originally developed at [exense](https://exense.ch) to power the [Step automation platform](https://step.exense.ch), the framework is open for external use and contributions.

## Overview

`step-framework` abstracts away common application server concerns — persistence, REST APIs, authentication, access control, audit logging, and time series metrics — so that teams can focus on business logic rather than infrastructure plumbing.

## Modules

| Module | Artifact ID | Description |
|--------|-------------|-------------|
| Core | `step-framework-core` | Foundational utilities: async execution, plugin system, classpath scanning, and authentication abstractions |
| Model | `step-framework-model` | Shared data model: entity definitions, accessors, collection filters, and object enrichers |
| Collections | `step-framework-collections` | Database-agnostic collection API with in-memory and filesystem implementations, a query language (QL), and schema migration support |
| Collections – MongoDB | `step-framework-collections-mongodb` | MongoDB backend for the collections API (via MongoJack) |
| Collections – PostgreSQL | `step-framework-collections-postgresql` | PostgreSQL backend for the collections API (via HikariCP + JDBC) |
| Server | `step-framework-server` | RESTful server framework built on Jersey/Jetty with Swagger/OpenAPI, access control, security, and audit logging |
| Server Plugins | `step-framework-server-plugins` | Plugin and version management infrastructure for server extensions |
| Time Series | `step-framework-timeseries` | Time series engine: metric definitions, time bucketing, data ingestion, aggregation, and querying |

## Requirements

- Java 11 or later
- Maven 3.6 or later

## Getting Started

Add the BOM (Bill of Materials) to your project to manage dependency versions consistently:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>ch.exense.step</groupId>
      <artifactId>step-framework</artifactId>
      <version>VERSION</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

Then add only the modules you need:

```xml
<!-- Core utilities -->
<dependency>
  <groupId>ch.exense.step</groupId>
  <artifactId>step-framework-core</artifactId>
</dependency>

<!-- Collections with MongoDB backend -->
<dependency>
  <groupId>ch.exense.step</groupId>
  <artifactId>step-framework-collections-mongodb</artifactId>
</dependency>

<!-- REST server -->
<dependency>
  <groupId>ch.exense.step</groupId>
  <artifactId>step-framework-server</artifactId>
</dependency>
```

Replace `VERSION` with the latest release available on [Maven Central](https://central.sonatype.com/artifact/ch.exense.step/step-framework).

## Building from Source

```bash
git clone https://github.com/exense/step-framework.git
cd step-framework
mvn clean install
```

To skip tests during the build:

```bash
mvn clean install -DskipTests
```

## Step Ecosystem

`step-framework` is part of the [Step](https://step.dev) open-source automation platform. Related repositories:

| Repository | Description |
|------------|-------------|
| [step](https://github.com/exense/step) | Core backend — the main Step orchestration platform built on top of this framework |
| [step-grid](https://github.com/exense/step-grid) | Distributed execution grid for agent-based keyword execution |
| [step-api](https://github.com/exense/step-api) | Java API for writing Step Keywords |

For platform-level documentation see the [Step knowledgebase](https://step.dev/knowledgebase).

## Contributing

Contributions are welcome. Please open an issue to discuss a bug or feature request before submitting a pull request. All submissions are expected to include appropriate test coverage.

## License

This project is licensed under the [Apache License 2.0](LICENSE.txt).