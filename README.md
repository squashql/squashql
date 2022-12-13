# AITM 

[logo]

![build](https://github.com/paulbares/aitm/actions/workflows/ci.yml/badge.svg?branch=main)
![activity](https://img.shields.io/github/commit-activity/m/paulbares/aitm/main)
![license](https://img.shields.io/github/license/paulbares/aitm)
---

AITM is an open-source SQL query engine specialized in what-if analysis, building multi-dimensional queries to help
back-end developers make the most of any SQL database, and front-end developers easily configure their own metrics in
the UI.

- AITM is a middleware between [different popular SQL databases](#compatibility) and multiple clients/front end
- It builds [queries that were not possible or cumbersome in SQL](./QUERY.md#complex-comparison)
- It helps front-end developers run SQL queries in their own language in [TypeScript](https://www.typescriptlang.org/)

## Compatibility

AITM is currently compatible with [Apache Spark](https://spark.apache.org/), [ClickHouse](https://clickhouse.com/) and [BigQuery](https://cloud.google.com/bigquery/). 

## API

AITM exposes two http endpoints to interrogate your database.

1. `GET  /metadata`: to retrieve the list of tables and fields available
2. `POST /query`: to execute queries that accepts a json object representing the query to execute

To use those endpoints, AITM provides a [TypeScript](https://www.typescriptlang.org/) library with all you need:

```typescript
import {count, from, Querier} from "aitm-js-query"

const querier = new Querier("http://localhost:8080");

querier.getMetadata().then(response => {
  console.log(response)
})

const query = from("myTable")
        .select(["col1"], [], [count])
        .build()

querier.execute(query).then(response => {
  console.log(response)
})
```

See [this page](./QUERY.md) to learn more about the API.

## Prerequisites

### Java

You need to have Java 17:

- [JDK 17](https://openjdk.java.net/projects/jdk/17/)

### Node.js and NPM

If you need to build the TypeScript library locally, you need to have Node installed.

- [Node.js](https://nodejs.org/)

## Getting started

- Install prerequisites (see above)
- Build the project

```
mvn -pl :aitm-sandbox -am clean install -DskipTests -Pspring-boot
```

- Launch the project with the following command.

```
java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Ddataset.path=sandbox/src/main/resources/data/saas.csv -jar sandbox/target/aitm-sandbox-0.1-SNAPSHOT.jar
```

Server address is: `http://localhost:8080`. Once the server is up and running, you can start [executing queries](./QUERY.md).  

## Testing

To run the tests you will need:

- [Docker](https://www.docker.com/). The `docker` service should be running when launching the tests with maven.

Run:

```
mvn test
```

## Contributing

Before contributing to AITM, please read our [contributing guidelines](CONTRIBUTING.md).
