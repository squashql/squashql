##  Summary 

AITM provides a simple interface in Typescript for building SQL-like queries specifically made for AITM. 

### Goal

Enable developers to quickly write queries understandable by AITM using a syntax closed to SQL with 
some slight differences though

### Non-Goal

It is not a goal to do any kind of validation of SQL correctness and to check the inputs provided by the developer

## How to start: entry point

The entry point of the library for building queries is `from`. A table must first be added to the query.
```typescript
import {
  from,
} from "aitm-js-query"

from("myTable")
```

## Select

Note: the concepts of measure and columnSet are detailed below.

Selects columns from the table to be displayed and the measures to compute. Note that the columns and columnSets
added to select are automatically injected to the groupBy clause of the query: aggregated results are then grouped by
the columns and columnSets indicated.

```typescript
import {
  from, sum, avg
} from "aitm-js-query"

const q = from("myTable")
        .select(
                ["col1", "col2"], // list of columns
                [], // list of columnSets
                [sum("alias1", "col3"), avg("alias2", "col4")] // list of measures
        )
        .build();
```

The above example is equivalent to the following SQL

```sql
SELECT col1, col2, sum(col3) as alias1, sum(col4) as alias2 FROM myTable GROUP BY col1, col2
```

## Filtering

Queries can be filtered by using condition operators `_in, eq, neq, and, or, lt, le, gt, ge, isNull, isNotNull`

```typescript
import {
  from, sum, avg, _in, eq
} from "aitm-js-query"

const q = from("myTable")
        .where("col1", _in(["a", "b"]))
        .where("col2", eq("c"))
        .select(
                ["col1", "col2"],
                [],
                [sum("alias1", "col3"), avg("alias2", "col4")])
        .build();
```

```sql
SELECT col1, col2, sum(col3) as alias1, sum(col4) as alias2 FROM myTable WHERE col1 IN ('a', 'b') AND col2='c' GROUP BY col1, col2
```

## Joining Tables

Tables can be joined with other tables by using `innerJoin` and `leftOuterJoin` immediately followed by `on` operator (equivalent to `ON` clause in SQL)

### Single join / Single join condition

```typescript
const q = from("myTable")
        .innerJoin("refTable")
        .on("myTable", "id", "refTable", "id")
        .select(["myTable.col", "refTable.col"], [], [])
        .build()
```

```sql
SELECT myTable.col, refTable.col FROM myTable INNER JOIN refTable ON myTable.id = refTable.id
```

### Single join / Multiple join condition

```typescript
const q = from("myTable")
        .innerJoin("refTable")
        .on("myTable", "id1", "refTable", "id1")
        .on("myTable", "id2", "refTable", "id2")
        .select(["myTable.col", "refTable.col"], [], [])
        .build()
```

```sql
SELECT myTable.col, refTable.col FROM myTable INNER JOIN refTable ON myTable.id1 = refTable.id1 AND myTable.id2 = refTable.id2 
```

### Multiple join

```typescript
const q = from("myTable")
        .innerJoin("refTable")
        .on("myTable", "id", "refTable", "id")
        .leftOuterJoin("otherTable")
        .on("myTable", "id", "otherTable", "key1")
        .on("refTable", "id", "otherTable", "key2")
        .select(["myTable.col", "refTable.col"], [], [])
        .build()
```

```sql
SELECT myTable.col, refTable.col FROM myTable INNER JOIN refTable ON myTable.id = refTable.id LEFT OUTER JOIN otherTable ON myTable.id = otherTable.key1 AND refTable.id = otherTable.key2
```

## Subqueries in FROM Clause (also known as inner or nested queries)

A subquery can be nested in the `FROM` clause. Start by using `fromSubQuery` instead of 'from'

```typescript
import {
  from, fromSubQuery, sum, avg, 
} from "aitm-js-query"

const subQuery = from("student")
        .select(["name"], [], [sum("score_sum", "score")])
        .build()

const query = fromSubQuery(subQuery)
        .select([], [], [avg("result", "score_sum")])
        .build()
```

Example: Return the average total for all students

```sql
SELECT AVG(score_sum) AS result FROM (SELECT SUM(score) AS score_sum FROM student GROUP BY name) AS t;
```
(from [https://mariadb.com/kb/en/subqueries-in-a-from-clause/](https://mariadb.com/kb/en/subqueries-in-a-from-clause/))

## Measures

TODO 

## ColumnSets

TODO 
