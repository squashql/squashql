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
- It makes calculations that were not possible or cumbersome in SQL easy to perform. See [comparison measures](documentation/QUERY.md#complex-comparison)
- It helps front-end developers build and run SQL queries from their front-end or node.js applications thanks to its [TypeScript SQL-like query builder](https://www.npmjs.com/package/@squashql/squashql-js) 
- With its "write once, run everywhere" approach, it is a great solution for those who need to quickly and efficiently query data from multiple databases.

![ide ts squashql](https://user-images.githubusercontent.com/5783183/215964358-37814efa-f315-4de5-97cd-cefce537caaa.gif)

## Try SquashQL

You can try SquashQL directly from your web browser with [our showcase project](https://github.com/squashql/squashql-showcase/blob/main/TUTORIAL.md). No need to install anything!

## Compatibility

SquashQL is currently compatible with the following SQL databases: [Apache Spark](https://spark.apache.org/), 
[ClickHouse](https://clickhouse.com/), [BigQuery](https://cloud.google.com/bigquery/), [Snowflake](https://www.snowflake.com/en/) 
and [DuckDB](https://duckdb.org/). 

## API

SquashQL provides an easy-to-use Typescript library to write SQL-like queries. See the [full documentation here](documentation/QUERY.md).

The library can be downloaded [here](https://www.npmjs.com/package/@squashql/squashql-js).

```typescript
import {
    from, avg
} from "@squashql/squashql-js"

const q = from("Orders")
    .select(["customer_id"], [], [avg("average_spends", "amount")])
    .build();
```

SquashQL comes with a [Spring Controller](./server/src/main/java/io/squashql/spring/web/rest/QueryController.java) to expose 
http endpoints for metadata discovery and query execution. 

To use those endpoints, you can use the `Querier` object from [squashql-js](https://www.npmjs.com/package/@squashql/squashql-js) Typescript library.

The object `Querier` uses [Axios](https://axios-http.com/) under the hood as HTTP Client. 
[Optional configuration](https://axios-http.com/docs/req_config) can be provided like this:

```typescript
const axiosConfig = {
  timeout: 10000
}
const serverUrl = "http://localhost:8080";
const querier = new Querier(serverUrl, axiosConfig)
```

### Metadata
`querier.getMetadata()`: to return the list of tables and fields available

```json
{
  "stores": [{
    "name": "budget",
    "fields": [{
      "name": "Income / Expenditure",
      "expression": "Income / Expenditure",
      "type": "java.lang.String"
    },
      {
        "name": "Category",
        "expression": "Category",
        "type": "java.lang.String"
      }
      ...
    ],
    ...
  }],
  "aggregationFunctions": ["ANY_VALUE", "AVG", "CORR", "COUNT", "COVAR_POP", "MAX", "MEDIAN", "MIN", "MODE", "STDDEV_POP", "STDDEV_SAMP", "SUM", "VAR_POP", "VAR_SAMP"],
  "measures": []
}
```

### QUERY

#### Regular query

To execute a query. It accepts a json object built with the Typescript library and returns a JSON
object representing the result table of the computation. The object returns is of type [QueryResult](https://github.com/squashql/squashql/blob/main/js/typescript-library/src/querier.ts#L53)

```typescript
const income = sumIf("Income", "Amount", criterion("Income / Expenditure", eq("Income")))
const expenditure = sumIf("Expenditure", "Amount", criterion("Income / Expenditure", neq("Income")))
const query = from("budget")
        .where(criterion("Scenario", eq("b")))
        .select(["Year", "Month", "Category"], [], [income, expenditure])
        .build()
querier.execute(query).then(r => console.log(JSON.stringify(r)));
```
 
The `execute` method accepts two other arguments:
- `PivotConfig` (detailed [below](#pivot-table-query))
- `stringify` default value is false. If true, it returns the result as a string instead of a JSON object that can be printed 
to display the result table:

```typescript
querier.execute(query, undefined, true).then(r => console.log(r));
```

```
+------+-------+---------------------------------+--------+--------------------+
| Year | Month |                        Category | Income |        Expenditure |
+------+-------+---------------------------------+--------+--------------------+
| 2022 |     1 |            Cigarettes & Alcohol |    NaN | 54.870000000000005 |
| 2022 |     1 |                  Current Income | 1930.0 |                NaN |
| 2022 |     1 | Media & Clothes & Food Delivery |    NaN |             118.75 |
| 2022 |     1 |             Minimum expenditure |    NaN |            1383.87 |
...
```

#### Pivot table query

SquashQL brings the ability to execute any SQL queries and transform their results into a format suitable for pivot table visualization. Check out [this example](https://github.com/squashql/squashql-showcase/blob/main/TUTORIAL.md#pivot-table) in our tutorial.

To execute a query whose result will be enriched with totals and subtotals to be able to display the result as a pivot table.
It accepts a json object built with the Typescript library and returns a JSON object representing the result table of the computation. 
The object returns is of type [PivotTableQueryResult](https://github.com/squashql/squashql/blob/main/js/typescript-library/src/querier.ts#L59).

To enable the pivot table feature, a `PivotConfig` parameter needs to be pass to the `execute` method: 
```typescript
const pivotConfig: PivotConfig = {
  rows: ["Category"],
  columns: ["Year", "Month"]
}
```
It is used by SquashQL to know which totals and subtotals needs to be computed. The union of the two lists rows and columns
must be exactly equal to the list of columns provided in the select.  

```typescript
const income = sumIf("Income", "Amount", criterion("Income / Expenditure", eq("Income")))
const expenditure = sumIf("Expenditure", "Amount", criterion("Income / Expenditure", neq("Income")))
const query = from("budget")
        .where(criterion("Scenario", eq("b")))
        .select(["Year", "Month", "Category"], [], [income, expenditure])
        .build()
querier.execute(query, pivotConfig).then(r => console.log(JSON.stringify(r)));
```

With `stringify = true`
```typescript
querier.execute(query, pivotConfig, true).then(r => console.log(r));
```

```
+---------------------------------+-------------+--------------------+--------+--------------------+--------+--------------------+--------+ ...
|                            Year | Grand Total |        Grand Total |   2022 |               2022 |   2022 |               2022 |   2022 | ...
|                           Month | Grand Total |        Grand Total |  Total |              Total |      1 |                  1 |      2 | ...
|                        Category |      Income |        Expenditure | Income |        Expenditure | Income |        Expenditure | Income | ...
+---------------------------------+-------------+--------------------+--------+--------------------+--------+--------------------+--------+ ...
|                     Grand Total |     11820.0 | 11543.240000000005 | 5790.0 |  5599.740000000005 | 1930.0 | 1823.0399999999997 | 1930.0 | ...
|            Cigarettes & Alcohol |         NaN | 335.82000000000005 |    NaN | 161.82000000000002 |    NaN | 54.870000000000005 |    NaN | ...
|                  Current Income |     11820.0 |                NaN | 5790.0 |                NaN | 1930.0 |                NaN | 1930.0 | ...
| Media & Clothes & Food Delivery |         NaN |             813.91 |    NaN | 393.90999999999997 |    NaN |             118.75 |    NaN | ...
|             Minimum expenditure |         NaN |  8573.500000000002 |    NaN |  4159.000000000002 |    NaN |            1383.87 |    NaN | ...
|                Outing Lifestyle |         NaN |             1057.7 |    NaN |              509.7 |    NaN |             141.38 |    NaN | ...
|             Sport & Game & misc |         NaN |             762.31 |    NaN |             375.31 |    NaN | 124.17000000000002 |    NaN | ...
+---------------------------------+-------------+--------------------+--------+--------------------+--------+--------------------+--------+ ...
```

#### Drilling across query

To execute *Drilling across* query i.e. querying two fact tables. The two results are aligned by
performing a sort-merge operation on the common attribute column headers. 
The object returns is of type [QueryResult](https://github.com/squashql/squashql/blob/main/js/typescript-library/src/querier.ts#L53).

```typescript
const myFirstQuery = from("myTable")
        .select(["col1"], [], [count])
        .build()
const mySecondQuery = from("otherTable")
        .select(["col1", "col2"], [], [sum("alias", "field")])
        .build()
querier.executeQueryMerge(new QueryMerge(myFirstQuery, mySecondQuery)).then(response => console.log(response))
```

Full documentation of [Drilling across in the dedicated page](./documentation/DRILLING-ACROSS.md).

### Under the hood

SquashQL helps you executing multi-dimensional queries compatible with several databases. The syntax is closed to SQL but... 

> What happens exactly when the query is sent to SquashQL?

Once the query is received by SquashQL server, it is analyzed and broken down into one or multiple *elementary* 
queries that can be executed by the underlying database. Before sending those queries for execution, SquashQL first looks
into its query cache (see [CaffeineQueryCache](https://github.com/squashql/squashql/blob/main/core/src/main/java/io/squashql/query/CaffeineQueryCache.java))
to see if the result of each *elementary* query exist. If it does, the result is returned immediately. If it does not, 
the elementary query is translated into compatible SQL statement, sent and executed by the database. 
The intermediary results are cached into SquashQL query cache for future reuse and used to compute the final query result. 

### Configuration

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
