# Configuration

To connect SquashQL to your database you will first have to import the associated maven module and defined in your
java project a `QueryEngine` and `Datasatore` by picking the correct implementations. Then declare a bean that returns
the `QueryEngine` instance.

Find a ready-to-use example with DuckDB and Spring Boot [here](https://github.com/squashql/squashql-showcase).

#### DuckDB

Maven

```xml

<dependency>
  <groupId>io.squashql</groupId>
  <artifactId>squashql-duckdb</artifactId>
  <version>${squashql.version}</version>
</dependency>
```

Java

```
DuckDBDatastore ds = new DuckDBDatastore();
DuckDBQueryEngine qe = new DuckDBQueryEngine(ds);
```

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
BigQueryDatastore ds = new BigQueryServiceAccountDatastore(credentials, projectId, datasetName);
BigQueryEngine qe = new BigQueryEngine(ds);
```

See how to create
a [credentials object in BigQuery documentation](https://cloud.google.com/bigquery/docs/authentication/service-account-file)

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

#### PostgreSQL

Maven

```xml

<dependency>
  <groupId>io.squashql</groupId>
  <artifactId>squashql-postgresql</artifactId>
  <version>${squashql.version}</version>
</dependency>
```

Java

```
String jdbcUrl = jdbc:postgresql://localhost:51215/<database>; // to be defined
Properties properties = ... // to be defined, it contains in particular the credentials (user, password...)
PostgreSQLDatastore ds = new PostgreSQLDatastore(jdbcUrl, properties);
PostgreSQLQueryEngine qe = new PostgreSQLQueryEngine(ds);
```
