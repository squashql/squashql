<p align="center">
  <img width="512" src="./documentation/assets/logo_horizontal.svg">
</p>

<p align="center">
  <a href="https://github.com/squashql/squashql/actions">
    <img src="https://github.com/squashql/squashql/actions/workflows/ci.yml/badge.svg?branch=main" alt="Github Actions Badge">
  </a>
  <a>
    <img alt="GitHub commit activity" src="https://img.shields.io/github/commit-activity/m/squashql/squashql">
  </a>
  <a>
    <img alt="Discord chat" src="https://img.shields.io/discord/1051535068637700196?label=discord">
  </a> 
</p>

---

SquashQL is an open-source SQL query engine specialized in what-if analysis, building multi-dimensional queries to help
back-end developers make the most of any SQL database, and front-end developers easily configure their own metrics in
the UI.

- It is a middleware between [a SQL database](#compatibility) and multiple clients/front end. Heavy computation is delegated to the underlying database. 
- It makes calculations that were not possible or cumbersome in SQL easy to perform. See [comparison measures](./QUERY.md#complex-comparison)
- It helps front-end developers build and run SQL queries from their front-end or node.js applications thanks to its [TypeScript SQL-like query builder](https://www.npmjs.com/package/@squashql/squashql-js) 
- With its "write once, run everywhere" approach, it is a great solution for those who need to quickly and efficiently query data from multiple databases.

![ide ts squashql](https://user-images.githubusercontent.com/5783183/215964358-37814efa-f315-4de5-97cd-cefce537caaa.gif)

## Try SquashQL

You can try SquashQL directly from your web browser with [our showcase project](https://github.com/squashql/squashql-showcase/blob/main/TUTORIAL.md). No need to install anything!

## Compatibility

SquashQL is currently compatible with the following SQL databases: [Apache Spark](https://spark.apache.org/), [ClickHouse](https://clickhouse.com/), [BigQuery](https://cloud.google.com/bigquery/) and [Snowflake](https://www.snowflake.com/en/). 

## API

SquashQL provides an easy-to-use Typescript library to write SQL-like queries. See the [full documentation here](./QUERY.md).

```typescript
import {
    from, avg
} from "@squashql/squashql-js"

const q = from("Orders")
    .select(["customer_id"], [], [avg("average_spends", "amount")])
    .build();
```

SquashQL comes with a [Spring Controller](./server/src/main/java/io/squashql/spring/web/rest/QueryController.java) to expose 
http endpoints.

1. `GET  /metadata`: it returns the list of tables and fields available
2. `POST /query`: to execute a query. It accepts a json object built with the Typescript library and returns a two-dimensional
array representing the result table of the computation. 
3. `POST /query-merge`: to execute *Drilling across* query i.e. querying two fact tables. The two results are aligned by 
performing a sort-merge operation on the common attribute column headers. More on [Drilling across](./documentation/DRILLING-ACROSS.md).

To use those endpoints, you can use the `Querier` object from squashql-js library.

```typescript
import {count, sum, from, Querier, QueryMerge} from "@squashql/squashql-js"

const serverUrl = "http://localhost:8080";
const querier = new Querier(serverUrl)

// Calling GET  /metadata
querier.getMetadata().then(response => console.log(response))

const myFirstQuery = from("myTable")
        .select(["col1"], [], [count])
        .build()

// POST /query with myFirstQuery as payload
querier.execute(myFirstQuery).then(response => console.log(response))

// POST /query-merge
const mySecondQuery = from("otherTable")
        .select(["col1", "col2"], [], [sum("alias", "field")])
        .build()
querier.executeQueryMerge(new QueryMerge(myFirstQuery, mySecondQuery)).then(response => console.log(response))
```

The object `Querier` uses [Axios](https://axios-http.com/) under the hood as HTTP
Client. [Additional configuration](https://axios-http.com/docs/req_config) can be
provided like this:

```typescript
const axiosConfig = {
  timeout: 10000
}
const serverUrl = "http://localhost:8080";
const querier = new Querier(serverUrl, axiosConfig)
```

### Configuration

To connect SquashQL to your database you will first have to import the associated maven module and defined in your 
java project a `QueryEngine` and `Datasatore` by picking the correct implementations. Then declare a bean that returns 
the `QueryEngine` instance.

Find a ready-to-use example with Apache Spark and Spring Boot [here](https://github.com/squashql/squashql-showcase).

#### Apache Spark

Maven
```xml
<dependency>
  <groupId>io.squashql</groupId>
  <artifactId>squashql-spark</artifactId>
  <version>${squashql.version}</version>
</dependency>
```

Java
```
SparkSession sparkSession = ...;// to be defined
SparkDatastore ds = new SparkDatastore(sparkSession);
SparkQueryEngine qe = new SparkQueryEngine(ds);
```

#### ClickHouse

Maven
```xml
<dependency>
  <groupId>io.squashql</groupId>
  <artifactId>squashql-clickhouse</artifactId>
  <version>${squashql.version}</version>
</dependency>
```

Java
```
String jdbcUrl = ...; // to be defined
ClickHouseDatastore ds = new ClickHouseDatastore(jdbcUrl);
ClickHouseQueryEngine qe = new ClickHouseQueryEngine(ds);
```

#### BigQuery

Maven
```xml
<dependency>
  <groupId>io.squashql</groupId>
  <artifactId>squashql-bigquery</artifactId>
  <version>${squashql.version}</version>
</dependency>
```

Java
```
ServiceAccountCredentials credentials = ...; // to be defined
String projectId = ...; // to be defined
String datasetName = ...;// to be defined
BigQueryDatastore ds = new BigQueryDatastore(credentials, projectId, datasetName);
BigQueryQueryEngine qe = new BigQueryQueryEngine(ds);
```

See how to create a [credentials object in BigQuery documentation](https://cloud.google.com/bigquery/docs/authentication/service-account-file)

#### Snowflake

Maven
```xml
<dependency>
  <groupId>io.squashql</groupId>
  <artifactId>squashql-snowflake</artifactId>
  <version>${squashql.version}</version>
</dependency>
```

Java
```
String jdbcUrl = jdbc:snowflake://<account_identifier>.snowflakecomputing.com; // to be defined
String database = ...; // to be defined
String schema = ...; // to be defined
Properties properties = ... // to be defined, it contains in particular the credentials (user, password, warehouse...)
SnowflakeDatastore ds = new SnowflakeDatastore(jdbcUrl, database, schema, properties);
SnowflakeQueryEngine qe = new SnowflakeQueryEngine(ds);
```

## Prerequisites

### Java

You need to have Java 17:

- [JDK 17](https://openjdk.java.net/projects/jdk/17/)

### Node.js and NPM

If you need to build the TypeScript library locally, you need to have Node installed.

- [Node.js](https://nodejs.org/)

## Testing

To run the tests you will need:

- [Docker](https://www.docker.com/). The `docker` service should be running when launching the tests with maven.

Run:

```
mvn test
```

## Contributing

Before contributing to SquashQL, please read our [contributing guidelines](CONTRIBUTING.md). 
If you can't find something you'd want to use or want to share your ideas, please open an issue or join our [discord server](https://discord.gg/p7dg2wEwFs).
