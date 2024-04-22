
## Summary

The library is available here [https://www.npmjs.com/package/@squashql/squashql-js](https://www.npmjs.com/package/@squashql/squashql-js).

SquashQL provides a simple interface in Typescript for building SQL-like queries specifically made for SquashQL. In each section, 
a Typescript code snippet is shown along with a SQL query to fix ideas, but it does not mean that this will be the SQL 
query executed because the generated SQL query depends on the underlying database and the type of computation to perform. 

### Goal

Enable developers to quickly write queries understandable by SquashQL using a syntax closed to SQL with
some slight differences though

### Non-Goal

It is not a goal to do any kind of validation of SQL correctness and to check the inputs provided by the developer

## How to start: entry point

The entry point of the library for building queries is `from`. A table must first be added to the query.

```typescript
import {
  from,
} from "@squashql/squashql-js"

from("myTable")
```

## Select

Note: the concepts of measure and columnSet are detailed below.

Selects columns from the table to be displayed and the measures to compute. Note that the **columns and columnSets
added to `select` are automatically injected to the groupBy clause of the query** to avoid repetitive and verbose code. 
Aggregated results are then grouped by the columns and columnSets indicated.

The first arguments of the `select` is an array of `Field`s. Most of the time, you will use `TableField`s to represent
fields belonging to a given table. It takes a single argument: a string that is the concatenation of the table it belongs 
and its name. Since it could be cumbersome to declared by hand all fields, we provide a tool to generate them for you:
https://github.com/squashql/squashql-codegen.

```typescript
import {
  from, sum, avg, Field, TableField
} from "@squashql/squashql-js"

const col1: Field = new TableField("myTable.col1")
const col2: Field = new TableField("myTable.col2")
const col3: Field = new TableField("myTable.col3")
const col4: Field = new TableField("myTable.col4")

const q = from("myTable")
        .select(
                [col1, col2], // list of columns
                [], // list of columnSets
                [sum("alias1", myTable.col3), avg("alias2", myTable.col4)] // list of measures
        )
        .build()
```

Or with [squashql-codegen](https://github.com/squashql/squashql-codegen):

```typescript
class MyTable {
  readonly _name: string = "myTable"
  readonly col1: TableField = new TableField("myTable.col1")
  readonly col2: TableField = new TableField("myTable.col2")
  readonly col3: TableField = new TableField("myTable.col3")
  readonly col4: TableField = new TableField("myTable.col4")
}
const myTable = new MyTable()

const q = from(myTable._name)
        .select(
                [myTable.col1, myTable.col2], // list of columns
                [], // list of columnSets
                [sum("alias1", myTable.col3), avg("alias2", myTable.col4)] // list of measures
        )
        .build()
```

The above example is equivalent to the following SQL

```sql
SELECT col1, col2, sum(col3) as alias1, sum(col4) as alias2
FROM myTable
GROUP BY col1, col2
```

For the rest of the documentation, I will assume all tables with their fields are declared in a file named `table.ts` that
looks like this:

```typescript
import {
  TableField
} from "@squashql/squashql-js"

class MyTable {
  readonly _name: string = "myTable"
  readonly id: TableField = new TableField("myTable.id")
  readonly col1: TableField = new TableField("myTable.col1")
  readonly col2: TableField = new TableField("myTable.col2")
  readonly col3: TableField = new TableField("myTable.col3")
  readonly col4: TableField = new TableField("myTable.col4")
}

class RefTable {
  readonly _name: string = "refTable"
  readonly id: TableField = new TableField("refTable.id")
  readonly col1: TableField = new TableField("refTable.col1")
}

const myTable = new MyTable()
const refTable = new RefTable()

export {
  myTable, refTable
}
```

To use it in another file, simply import it where you need it: `import {myTable} from "./table"`.

## Filtering

Queries can be filtered by using Criteria class. 

### WHERE - filtering records

A Criteria instance can contain a condition on a single field and can be built as so:
```typescript
import { criterion, eq } from "@squashql/squashql-js"
import {myTable} from "./table"

const criteria = criterion(myTable.col2, eq("c"))
```

Several criteria can be chained with `AND` or `OR` by using the methods `any` and `all`

```typescript
import {
  from, sum, avg, _in, eq, criterion, all
} from "@squashql/squashql-js"
import {myTable} from "./table"

const q = from(myTable._name)
        .where(all([criterion(myTable.col1, _in(["a", "b"])), criterion(myTable.col2, eq("c"))]))
        .select(
                [myTable.col1, myTable.col2],
                [],
                [sum("alias1", myTable.col3), avg("alias2", myTable.col4)])
        .build()
```

```sql
SELECT col1, col2, sum(col3) as alias1, sum(col4) as alias2
FROM myTable
WHERE (col1 IN ('a', 'b') AND col2 = 'c')
GROUP BY col1, col2
```

Condition operators available: `eq, neq, lt, le, gt, ge, _in, isNull, isNotNull, like, and, or`.

If the field is an array, the condition `contains` can be used:

```
...
.where(criterion(myTable.myArray, contains(2)))
...
```

```sql
...
WHERE ARRAY_CONTAINS(myArray, 2)
...
```

### HAVING - filtering aggregates

> **Warning**
> Filtering can only be applied on [basic measure](#basic-measure)

A Criteria instance can contain a condition on a single [basic measure](#basic-measure) and can be built as so:
```typescript
import { criterion, sum, ge } from "@squashql/squashql-js"
import {myTable} from "./table"

const measure = sum("sum", myTable.col1)
const criteria = criterion(measure, ge(0))
```

Several criteria can be chained with AND or OR by using the methods `any` and `all`

```typescript
import {
  from, sum, avg, gt, eq, havingCriterion, all
} from "@squashql/squashql-js"
import {myTable} from "./table"

const measure1 = sum("alias1", myTable.col3)
const measure2 = avg("alias2", myTable.col4)
const q = from(myTable._name)
        .select(
                [myTable.col1, myTable.col2],
                [],
                [measure1, measure2])
        .having(all([havingCriterion(measure1, gt(10)), havingCriterion(measure2, eq(5))]))
        .build()
```

```sql
SELECT col1, col2, sum(col3) as alias1, avg(col4) as alias2
FROM myTable
GROUP BY col1, col2
HAVING sum(col3) > 10 AND avg(col4) = 5
```

## Ordering

To avoid repetitive and verbose code, ordering of table result rows is automatically performed by SquashQL. It's
order by all columns in the query from left to right following natural order.


```typescript
import {
  from, sum, OrderKeyword
} from "@squashql/squashql-js"
import {myTable} from "./table"

const q = from(myTable._name)
        .select(
                [myTable.col1, myTable.col2, myTable.col3, myTable.col4],
                [],
                [sum("alias1", myTable.col5)])
        .build()
```

```sql
SELECT col1, col2, col3, col4, sum(col5) as alias1
FROM myTable
GROUP BY col1, col2, col3, col4
ORDER BY col1, col2, col3, col4,
```

However, ordering can be customized by using `orderBy` method.

```typescript
import {
  from, sum, OrderKeyword, tableField
} from "@squashql/squashql-js"
import {myTable} from "./table"

const q = from(myTable._name)
        .select(
                [myTable.col1, myTable.col2],
                [],
                [sum("alias1", myTable.col3)])
        .orderBy(tableField(myTable.col2), OrderKeyword.ASC)
        .build()
```

```sql
SELECT col1, col2, sum(col3) as alias1
FROM myTable
GROUP BY col1, col2
ORDER BY col2
```

## Joining Tables

Tables can be joined with other tables by using `join` immediately followed by `on` operator (equivalent to `ON` clause
in SQL) with a criterion to define the join condition i.e. the way the two tables are related
in the query.
Two types of join are supported: `INNER` and `LEFT`.

### Single join / Single join condition

```typescript
import {
  from, JoinType, ConditionType, criterion_
} from "@squashql/squashql-js"
import {myTable, refTable} from "./table"

const q = from(myTable._name)
        .join(refTable._name, JoinType.INNER)
        .on(criterion_(myTable.id, refTable.id, ConditionType.EQ))
        .select([myTable.col1, refTable.col1], [], [])
        .build()
```

```sql
SELECT myTable.col1, refTable.col1
FROM myTable INNER JOIN refTable ON myTable.id = refTable.id
```

### Single join / Multiple join condition

For multiple condition, chain the criteria with `all`.

```typescript
import {
  from, JoinType, ConditionType, criterion_, all
} from "@squashql/squashql-js"
import {myTable, refTable} from "./table"

const q = from(myTable._name)
        .join(refTable._name, JoinType.INNER)
        .on(all([
          criterion_(myTable.id1, refTable.id1, ConditionType.EQ),
          criterion_(myTable.id2, refTable.id2, ConditionType.EQ)
        ]))
        .select([myTable.col1, refTable.col1], [], [])
        .build()
```

```sql
SELECT myTable.col1, refTable.col1
FROM myTable INNER JOIN refTable ON myTable.id1 = refTable.id1 AND myTable.id2 = refTable.id2 
```

If your database supports non-equi joins, the `ConditionType` can be changed in the query. For instance:

```typescript
const q = from(myTable._name)
        .join(refTable._name, JoinType.INNER)
        .on(all([
          criterion_(myTable.kpi, refTable.min, ConditionType.GE),
          criterion_(myTable.kpi, refTable.max, ConditionType.LT)
        ]))
        .select([myTable.col1, refTable.col1], [], [])
        .build()
```

```sql
SELECT myTable.col1, refTable.col1
FROM myTable LEFT JOIN refTable ON myTable.kpi >= refTable.min AND myTable.kpi < refTable.max 
```

### Multiple join

```typescript
import {
  from
} from "@squashql/squashql-js"
import {myTable, refTable, otherTable} from "./table"

const q = from(myTable._name)
        .join(refTable._name, JoinType.INNER)
        .on(criterion_(myTable.id, refTable.id, ConditionType.EQ))
        .join(otherTable._name, JoinType.LEFT)
        .on(all([
          criterion_(myTable.id, otherTable.key1, ConditionType.EQ),
          criterion_(refTable.id, otherTable.key2, ConditionType.EQ)
        ]))
        .select([myTable.col1, refTable.col1], [], [])
        .build()
```

```sql
SELECT myTable.col1, refTable.col1
FROM myTable
       INNER JOIN refTable ON myTable.id = refTable.id
       LEFT OUTER JOIN otherTable ON myTable.id = otherTable.key1 AND refTable.id = otherTable.key2
```

### Joining on virtual table created on-the-fly at query time

You can define and use a virtual table in the query by joining it to an existing table. Such table is not materialized
in the database and only exists during the query execution time. Note the join operation is performed by the underlying
database.

Start by describing your virtual table in Typescript along with the data it contains. For instance, to define a table
named prioryLevels with 3 columns named priority, min and max and three rows:

```typescript
const records = [
  ["low", 0, 3],
  ["medium", 3, 7],
  ["high", 7, 11],
];
const prioryLevels = new VirtualTable("prioryLevels", ["priority", "min", "max"], records)
```

Once defined, you can use the `VirtualTable` object in the query by joining it to another table with the `joinVirtual` 
operator that takes a `VirtualTable` object as argument.

```typescript
const q = from("tasks")
        .joinVirtual(prioryLevels, JoinType.INNER)
        .on(all([
          criterion_(new TableField("tasks.priority"), new TableField("prioryLevels.min"), ConditionType.GE),
          criterion_(new TableField("tasks.priority"), new TableField("prioryLevels.max"), ConditionType.LT)
        ]))
        .select([new TableField("prioryLevels.priority")], [], [count])
        .build()
```

```sql
WITH prioryLevels AS (
  SELECT 'low' AS `priority`, 0 AS `min`, 3 AS `max` 
  UNION ALL
  SELECT 'medium' AS `priority`, 3 AS `min`, 7 AS `max` 
  UNION ALL
  SELECT 'high' AS `priority`, 7 AS `min`, 11 AS `max`
)
SELECT prioryLevels.priority
FROM tasks INNER JOIN prioryLevels ON tasks.priority >= prioryLevels.min AND tasks.priority < prioryLevels.max 
```

## Rollup

> The `ROLLUP` option allows you to include extra rows that represent the subtotals, which are commonly referred to as super-aggregate rows,
> along with the grand total row. By using the `ROLLUP` option, you can use a single query to generate multiple grouping sets.
(source https://www.sqltutorial.org/sql-rollup/)

```typescript
import {
  from, sum, TableField
} from "@squashql/squashql-js"

class PopulationTable {
  readonly _name: string = "populationTable"
  readonly continent: TableField = new TableField("populationTable.continent")
  readonly country: TableField = new TableField("populationTable.country")
  readonly city: TableField = new TableField("populationTable.city")
  readonly population: TableField = new TableField("populationTable.population")
}
const populationTable = new PopulationTable()

const query = from(populationTable._name)
        .select([populationTable.continent, populationTable.country, populationTable.city], [], [sum("pop", populationTable.population)])
        .rollup([populationTable.continent, populationTable.country, populationTable.city])
        .build()
```

```sql
SELECT continent,
       country,
       city,
       sum(population) as population
FROM populationTable
GROUP BY ROLLUP (continent, country, city)
```

Example
```
+-------------+-------------+-------------+------------+
|   continent |     country |        city |        pop |
+-------------+-------------+-------------+------------+
| Grand Total | Grand Total | Grand Total |       28.5 |
|          am |       Total |       Total |       17.0 |
|          am |      canada |       Total |        6.0 |
|          am |      canada |    montreal |        2.0 |
|          am |      canada |       otawa |        1.0 |
|          am |      canada |     toronto |        3.0 |
|          am |         usa |       Total |       11.0 |
|          am |         usa |     chicago |        3.0 |
|          am |         usa |         nyc |        8.0 |
|          eu |       Total |       Total |       11.5 |
|          eu |      france |       Total |        2.5 |
|          eu |      france |        lyon |        0.5 |
|          eu |      france |       paris |        2.0 |
|          eu |          uk |       Total |        9.0 |
|          eu |          uk |      london |        9.0 |
+-------------+-------------+-------------+------------+
```

### Partial rollup

Partial rollup reduces the number of subtotals by indicating which ones should be calculated.  

```typescript
const query = from(populationTable._name)
        .select([populationTable.continent, populationTable.country, populationTable.city], [], [sum("pop", populationTable.population)])
        .rollup([populationTable.country, populationTable.city])
        .build()
```

In the example above, subtotals for 'continent' won't be calculated i.e. the grand total in that case. 

```sql
SELECT continent,
       country,
       city,
       sum(population) as population
FROM populationTable
GROUP BY continent, ROLLUP (country, city)
```

Example
```
+-----------+---------+----------+------------+
| continent | country |     city | population |
+-----------+---------+----------+------------+
|        am |   Total |    Total |       17.0 |
|        am |  canada |    Total |        6.0 |
|        am |  canada | montreal |        2.0 |
|        am |  canada |    otawa |        1.0 |
|        am |  canada |  toronto |        3.0 |
|        am |     usa |    Total |       11.0 |
|        am |     usa |  chicago |        3.0 |
|        am |     usa |      nyc |        8.0 |
|        eu |   Total |    Total |       11.5 |
|        eu |  france |    Total |        2.5 |
|        eu |  france |     lyon |        0.5 |
|        eu |  france |    paris |        2.0 |
|        eu |      uk |    Total |        9.0 |
|        eu |      uk |   london |        9.0 |
+-----------+---------+----------+------------+
```

## Subqueries in FROM Clause (also known as inner or nested queries)

A subquery can be nested in the `FROM` clause. Start by using `fromSubQuery` instead of 'from'

```typescript
import {
  from, fromSubQuery, sum, avg,
} from "@squashql/squashql-js"

const subQuery = from(student._name)
        .select([student.name], [], [sum("score_sum", student.score)])
        .build()

const query = fromSubQuery(subQuery)
        .select([], [], [avg("result", new TableField("score_sum"))])
        .build()
```
Note `score_sum` does not belong to any table but can be referenced with its name in a `TableField` object.

Example: Return the average total for all students

```sql
SELECT AVG(score_sum) AS result
FROM (SELECT SUM(score) AS score_sum FROM student GROUP BY name)
```

(from [https://mariadb.com/kb/en/subqueries-in-a-from-clause/](https://mariadb.com/kb/en/subqueries-in-a-from-clause/))

## Fields

The interface `Field` can be used to represent either:
- A column/field of a table: `TableField` e.g. `const a = new TableField("myTable.a")`
- A constant value: `ConstantField` e.g. `const one = new ConstantField(1)`
- An aliased column: `AliasedField` e.g. `const aliased = new AliasedField("myColumn")`
- An SQL function with a single operand: `FunctionField` e.g. `const func = new FunctionField("lower", new TableField("myTable.a"))`

Fields can be combined to produce other fields to be used in calculations and perform more complex computations.

```typescript
import {
  TableField,
  ConstantField,
  avg,
  from,
} from "@squashql/squashql-js"

const a = new TableField("myTable.a")
const rate = new TableField("myTable.rate")
const one = new ConstantField(1)
const myMeasure = avg("myMeasure", a.divide(one.plus(rate)))

const query = from("myTable")
        .select([], [], [myMeasure])
        .build()
```

```sql
SELECT AVG(myTable.a / (1 + myTable.rate)) AS myMeasure
FROM myTable
```

Fields can be aliased to change the name of the returning columns by using `as()`. Using `as()` on a `TableField` produces
a new instance of `TableField`.

```typescript
const id = new TableField("myTable.customerId").as("id")

const query = from("myTable")
        .select([id], [], [countRows])
        .build()
```

```sql
SELECT myTable.customerId AS id, count(*)
FROM myTable
```

`AliasedField` is useful when using subqueries to reference a field in the top query from the subquery.

```typescript
const f1 = new TableField("myTable.f1").as("f1_alias")
const f2 = new TableField("myTable.f2").as("f2_alias")
const subQuery = from("myTable")
        .select([f1, f2], [], [sum("score_sum", student.score)])
        .build()

const query = fromSubQuery(subQuery)
        .select([new AliasedField("f1_alias")], [], [avg("result", new TableField("score_sum"))])
        .build()
```

`FunctionField` is made to apply a transformation to a column. Only functions that take a single argument are supported 
for the time being. The library provides helpers to create useful functions. For instance with `const a = new TableField("myTable.a")`

```typescript
const a = new TableField("myTable.a")
const query = from("myTable")
        .select([lower(a), upper(a), currentDate()], [], [countRows])
        .build()
```

```sql
SELECT lower(myTable.a), upper(myTable.a), current_date() AS id, count(*) FROM myTable
```

SQL functions:

| function name | Description                  | Example        | Result     | Alias |
|---------------|------------------------------|----------------|------------|-------|
| upper         | Convert string to upper case | upper('Hello') | HELLO      | ucase | 
| lower         | Convert string to lower case | lower('Hello') | hello      | lcase | 
| current_date  | Current date                 | current_date() | 2024-10-25 |       | 


## Measures

A Measure represents aggregated values and is usually numeric. Measure can be split into two categories depending on 
where the calculation is performed.

- Basic measure
  - Aggregate measure
  - Expression measure
- Calculated measure
  - Elementary operations: addition, subtraction, multiplication and division
  - Constant
  - Complex operations: comparison

### Basic measure

A basic measure **is always computed by the underlying database**.

#### Aggregate measure

An aggregate measure is computed by applying an aggregation function over a list of field values such as `sum`, `min`, `max`, `avg`, `count`, `countDistinct`...

Aggregation can also be applied to only the rows matching a [condition](#filtering) with `sumIf`, `minIf`, `maxIf`, `avgIf`, `countIf`, `countDistinctIf`...

```typescript
import {
  sum,
  avg,
  sumIf,
  countDistinct,
  countDistinctIf,
  eq,
  criterion,      
} from "@squashql/squashql-js"

const amountSum = sum("sum_amount", myTable.amount)
const amountAvg = avg("avg_amount", myTable.amount)
const sales = sumIf("sales", "amount", criterion(myTable.incomeExpense, eq("Revenue")))
const distinctStores = countDistinct("distinct_stores", myTable.store)
const distinctOpenedStores = countDistinctIf("distinct_opened_stores", myTable.store, criterion(myTable.isOpen, eq(true)))

const query = from(myTable._name)
        .select([], [], [amountSum, amountAvg, sales, distinctStores, distinctStoresSellingFood])
        .build()
```

```sql
SELECT SUM(amount)                                                AS sum_amount,
       AVG(amount)                                                AS avg_amount,
       SUM(CASE WHEN IncomeExpense = 'Revenue' THEN amount 0 END) AS sales,
       SUM(DISTINCT(store))                                       AS distinct_stores,
       SUM(DISTINCT(CASE WHEN isOpen = true THEN store 0 END))    AS distinct_opened_stores
FROM myTable
```

#### Expression measure

An expression measure is a measure that accepts a raw sql expression as argument.

```typescript
import {
  ExpressionMeasure
} from "@squashql/squashql-js"

const expression = new ExpressionMeasure("myMeasure", "sum(price * quantity)")
const query = from("myTable")
        .select([], [], [expression])
        .build()
```

```sql
SELECT SUM(price * quantity) AS myMeasure FROM myTable
```

### Calculated measure

Unlike a basic measure, a calculated measure is computed by SquashQL (not the database) by fetching all the required values
from the underlying database before applying the defined calculation.
It is defined as the combination of other measures that can be either basic or not.

#### Elementary: addition, subtraction, multiplication, division and relative difference

```typescript
import {
  sum,
  multiply, divide, plus, minus, relativeDifference, TableField
} from "@squashql/squashql-js"

const a = new TableField("myTable.a")
const b = new TableField("myTable.b")
const aSum = sum("aSum", a)
const bSum = sum("bSum", b)
const square = multiply("square", aSum, aSum)
const twoTimes = plus("twoTimes", aSum, aSum)
const zero = minus("zero", aSum, aSum)
const one = divide("one", aSum, aSum)
const relDiff = relativeDifference("relDiff", aSum, bSum) // = (aSum - bSum) / bSum
```

Constant measures can be defined with `decimal` or `integer` operators:

```typescript
import {
  sum,
  decimal,
  TableField
} from "@squashql/squashql-js"

const a = new TableField("myTable.a")
const b = new TableField("myTable.b")
const aSum = sum("aSum", a)
const bSum = sum("bSum", b)
const ratio = divide("ratio", a, b)
const percent = multiply("percent", ratio, decimal(100)) 
```

#### Constant measure

Used to define measure with a constant value in order to combine it with other measures. See below.

```typescript
import {
  decimal, integer
} from "@squashql/squashql-js"

const oneHundredDecimal = decimal(100)
const oneHundredInteger = integer(100)
```

#### Complex: comparison

##### Time-series comparison

Comparison between "time" period like year, semester, quarter, month.

> **Warning**
> Values of such periods are expected to be integers. Semester must be 1 or 2, quarter must be within `[1;4]`, month must be within `[1;12]`.

Example: Compare sum of score of each student with previous semester given this dataset
```
+------+----------+---------+-------------+-------+
| year | semester |    name |        test | score |
+------+----------+---------+-------------+-------+
| 2022 |        1 |    Paul |     english |    73 |
| 2022 |        1 |    Paul | mathematics |    75 |
| 2022 |        1 | Tatiana |     english |    83 |
| 2022 |        1 | Tatiana | mathematics |    87 |
| 2022 |        2 |    Paul |     english |    70 |
| 2022 |        2 |    Paul | mathematics |    58 |
| 2022 |        2 | Tatiana |     english |    65 |
| 2022 |        2 | Tatiana | mathematics |    65 |
| 2023 |        1 |    Paul |     english |    82 |
| 2023 |        1 |    Paul | mathematics |    70 |
| 2023 |        1 | Tatiana |     english |    96 |
| 2023 |        1 | Tatiana | mathematics |    52 |
| 2023 |        2 |    Paul |     english |    89 |
| 2023 |        2 |    Paul | mathematics |    45 |
| 2023 |        2 | Tatiana |     english |    63 |
| 2023 |        2 | Tatiana | mathematics |    14 |
+------+----------+---------+-------------+-------+
```

```typescript
import {
  ComparisonMethod,
  from,
  sum,
  Semester,
  comparisonMeasureWithPeriod,
  TableField
} from "@squashql/squashql-js"

class Student {
  readonly _name: string = "student"
  readonly year: TableField = new TableField("student.year")
  readonly semester: TableField = new TableField("student.semester")
  readonly name: TableField = new TableField("student.name")
  readonly test: TableField = new TableField("student.test")
  readonly score: TableField = new TableField("student.score")
}
const student = new Student()

const scoreSum = sum("score_sum", student.score)
const comparisonScore = comparisonMeasureWithPeriod(
        "compare with previous year",
        ComparisonMethod.ABSOLUTE_DIFFERENCE,
        scoreSum,
        new Map([[student.semester, "s-1"]]),
        new Semester(student.semester, student.year))

const query = from(student._name)
        .select([student.year, student.semester, student.name], [], [scoreSum, comparisonScore])
        .build()
```

Result
```
+------+----------+---------+-----------+--------------------------------+
| year | semester |    name | score_sum | compare with previous semester |
+------+----------+---------+-----------+--------------------------------+
| 2022 |        1 |    Paul |       148 |                           null |
| 2022 |        1 | Tatiana |       170 |                           null |
| 2022 |        2 |    Paul |       128 |                            -20 |
| 2022 |        2 | Tatiana |       130 |                            -40 |
| 2023 |        1 |    Paul |       152 |                             24 |
| 2023 |        1 | Tatiana |       148 |                             18 |
| 2023 |        2 |    Paul |       134 |                            -18 |
| 2023 |        2 | Tatiana |        77 |                            -71 |
+------+----------+---------+-----------+--------------------------------+
```

`comparisonMeasureWithPeriod` method is used to create a special measure built to compare values of an underlying
measure
(third argument) with other values of the same measure. In this example, we want to compute the "absolute" difference
(hence `ComparisonMethod.ABSOLUTE_DIFFERENCE`) of score_sum for a given semester with score_sum of the previous semester
(relatively to the semester of a given row).

To indicate "previous semester", a "translation" or "shift" operator is passed to the
comparison function: `{"semester": "s-1", "year": "y"}`. `"semester": "s-1"` means 'take the current semester value and
remove 1'. You can use any translation that follows the same pattern" `s-2`, `s-3`, `s+1`, `s+2`... The translation
can be replaced with `first` to indicate the first semester.  
The `Semester` object is meant to make SquashQL understand the measure is working with a **time period** so that it knows
that the previous semester value of the 1st semester of 2023 is the 2nd semester of 2022.

Similarly to `Semester`, one can use `Year`, `Quarter`, `Month` to work with a different time period. The arguments
passed to build a time period object are
the name of the table columns necessary to unambiguously deduce from their values the time period it refers to.
For `Semester`,
the required information is year (should be an integer) and semester (also an integer) to know which half-year term it
refers
to.

Note: The columns used to build a time period object need to be added to the query in the select.

```typescript
import {
  ComparisonMethod,
  from,
  sum,
  multiply,
  decimal,
  Year,
  comparisonMeasureWithPeriod,
  TableField
} from "@squashql/squashql-js"

class Student {
  readonly _name: string = "student"
  readonly year: TableField = new TableField("student.year")
  readonly semester: TableField = new TableField("student.semester")
  readonly name: TableField = new TableField("student.name")
  readonly test: TableField = new TableField("student.test")
  readonly score: TableField = new TableField("student.score")
}
const student = new Student()

const scoreSum = sum("score_sum", student.score)
const comparisonScore = comparisonMeasureWithPeriod(
        "compare with previous year",
        ComparisonMethod.RELATIVE_DIFFERENCE,
        scoreSum,
        new Map([[student.year, "y-1"]]),
        new Year(student.year))

const query = from(student._name)
        .select([student.year, student.name], [], [scoreSum, multiply("progression in %", comparisonScore, decimal(100))])
        .build()
```

Result
```
+------+---------+-----------+--------------------+
| year |    name | score_sum |   progression in % |
+------+---------+-----------+--------------------+
| 2022 |    Paul |       276 |               null |
| 2022 | Tatiana |       300 |               null |
| 2023 |    Paul |       286 | 3.6231884057971016 |
| 2023 | Tatiana |       225 |              -25.0 |
+------+---------+-----------+--------------------+
```

##### Hierarchical / Parent-Child comparison

SquashQL introduces the concept of organizing hierarchically several columns in order to compare aggregates and sub-aggregates
compute at different levels of the lineage.

Example: compute the ratio of population of a city to its country and of a country to its continent. 
```
+-------------+-------------+-------------+------------+
|   continent |     country |        city |        pop |
+-------------+-------------+-------------+------------+
| Grand Total | Grand Total | Grand Total |       28.5 |
|          am |       Total |       Total |       17.0 |
|          am |      canada |       Total |        6.0 |
|          am |      canada |    montreal |        2.0 |
|          am |      canada |       otawa |        1.0 |
|          am |      canada |     toronto |        3.0 |
|          am |         usa |       Total |       11.0 |
|          am |         usa |     chicago |        3.0 |
|          am |         usa |         nyc |        8.0 |
|          eu |       Total |       Total |       11.5 |
|          eu |      france |       Total |        2.5 |
|          eu |      france |        lyon |        0.5 | 
|          eu |      france |       paris |        2.0 |
|          eu |          uk |       Total |        9.0 |
|          eu |          uk |      london |        9.0 |
+-------------+-------------+-------------+------------+
```

```typescript
import {
  ComparisonMethod,
  from,
  sum,
  comparisonMeasureWithParent,
  TableField
} from "@squashql/squashql-js"

class PopulationTable {
  readonly _name: string = "populationTable"
  readonly continent: TableField = new TableField("populationTable.continent")
  readonly country: TableField = new TableField("populationTable.country")
  readonly city: TableField = new TableField("populationTable.city")
  readonly population: TableField = new TableField("populationTable.population")
}
const populationTable = new PopulationTable()

const pop = sum("pop", populationTable.population)
const ancestors = [populationTable.continent, populationTable.country, populationTable.city]
const parentRatio = comparisonMeasureWithParent("parent ratio", ComparisonMethod.DIVIDE, pop, ancestors)
const gtRatio = comparisonMeasureWithGrandTotalAlongAncestors("gt ratio", ComparisonMethod.DIVIDE, pop, ancestors)
const query = from(populationTable._name)
        .select([populationTable.continent, populationTable.country, populationTable.city], [], [pop, parentRatio, gtRatio])
        .rollup([populationTable.continent, populationTable.country, populationTable.city])
        .build()
```

`comparisonMeasureWithParent` method is used to create a special measure built to compare values of an underlying
measure (third argument) with the parent values of the same measure. 

`comparisonMeasureWithGrandTotalAlongAncestors` method is used to create a special measure built to compare values of an underlying
measure (third argument) with the grand total values of the same measure along the ancestors.

Parenthood is indicated with the array of `ancestors` (fourth argument) which contains column names in "lineage reverse order".
Note the columns used to define the ancestors need to be passed to the select method but not necessary to the rollup method. 

Result
```
+-------------+-------------+-------------+------------+---------------------+---------------------+
|   continent |     country |        city | population |        parent ratio |            gt ratio |
+-------------+-------------+-------------+------------+---------------------+---------------------+
| Grand Total | Grand Total | Grand Total |       35.0 |                 1.0 |                 1.0 |
|          am |       Total |       Total |       24.0 |  0.6857142857142857 |  0.6857142857142857 |
|          am |      canada |       Total |       12.0 |                 0.5 | 0.34285714285714286 |
|          am |      canada |    montreal |        7.0 |  0.5833333333333334 |                 0.2 |
|          am |      canada |       otawa |        2.0 | 0.16666666666666666 | 0.05714285714285714 |
|          am |      canada |     toronto |        3.0 |                0.25 | 0.08571428571428572 |
|          am |         usa |       Total |       12.0 |                 0.5 | 0.34285714285714286 |
|          am |         usa |     chicago |        4.0 |  0.3333333333333333 | 0.11428571428571428 |
|          am |         usa |         nyc |        8.0 |  0.6666666666666666 | 0.22857142857142856 |
|          eu |       Total |       Total |       11.0 |  0.3142857142857143 |  0.3142857142857143 |
|          eu |      france |       Total |        3.0 |  0.2727272727272727 | 0.08571428571428572 |
|          eu |      france |        lyon |        1.0 |  0.3333333333333333 | 0.02857142857142857 |
|          eu |      france |       paris |        2.0 |  0.6666666666666666 | 0.05714285714285714 |
|          eu |          uk |       Total |        8.0 |  0.7272727272727273 | 0.22857142857142856 |
|          eu |          uk |      london |        8.0 |                 1.0 | 0.22857142857142856 |
+-------------+-------------+-------------+------------+---------------------+---------------------+
```

`comparisonMeasureWithGrandTotal` method is used to create a special measure built to compare values of an underlying
measure with the grand total values of the same measure.

```typescript
const gtRatio = comparisonMeasureWithGrandTotal("gt ratio", ComparisonMethod.DIVIDE, pop)
```

`comparisonMeasureWithParentOfAxis`: same as `comparisonMeasureWithParent` but the list of ancestors is deduced at query 
time by what's on the axes. When not using the pivot table API, only using `Axis.COLUMN` makes sense.

`comparisonMeasureWithTotalOfAxis`: same as `comparisonMeasureWithGrandTotalAlongAncestors` but the list of ancestors is 
deduced at query time by what's on the axes. When not using the pivot table API, only using `Axis.COLUMN` makes sense.

Examples with `comparisonMeasureWithParentOfAxis`.  

|         |        | total | 2023  | 2024  |
|---------|--------|-------|-------|-------|
| total   | total  | 44    | 23    | 21    |
| france  | total  | 31    | 15    | 16    |
| france  | paris  | 22    | 10    | 12    |
| france  | lyon   | 9     | 5     | 4     |
| germany | total  | 12    | 7     | 5     |
| germany | berlin | 7     | 4     | 3     |
| germany | munich | 5     | 3     | 2     |

`Axis = COLUMN`
```typescript
comparisonMeasureWithParentOfAxis("parent", DIVIDE, value, COLUMN)
```
|         |        | total | 2023  | 2024  |
|---------|--------|-------|-------|-------|
| total   | total  | 44/44 | 23/23 | 21/21 |
| france  | total  | 31/44 | 15/23 | 16/21 |
| france  | paris  | 22/31 | 10/15 | 12/16 |
| france  | lyon   | 9/31  | 5/15  | 4/16  |
| germany | total  | 12/44 | 7/23  | 5/21  |
| germany | berlin | 7/12  | 4/7   | 3/5   |
| germany | munich | 5/12  | 3/7   | 2/5   |

`Axis = ROW`
```typescript
comparisonMeasureWithParentOfAxis("parent", DIVIDE, value, ROW)
```
|         |        | total | 2023  | 2024  |
|---------|--------|-------|-------|-------|
| total   | total  | 44/44 | 23/44 | 21/44 |
| france  | total  | 31/31 | 15/31 | 16/31 |
| france  | paris  | 22/22 | 10/22 | 12/22 |
| france  | lyon   | 9/9   | 5/9   | 4/9   |
| germany | total  | 12/12 | 7/12  | 5/12  |
| germany | berlin | 7/7   | 4/7   | 3/7   |
| germany | munich | 5/5   | 3/5   | 2/5   |

Note that query filters on ancestors are ignored when computing a hierarchical measure. Using the following measure
```typescript
comparisonMeasureWithParentOfAxis("parent", DIVIDE, value, COLUMN)
```
with `where(criterion(city, eq("lyon"))` would lead to:

|         |        | total | 2023  | 2024  |
|---------|--------|-------|-------|-------|
| total   | total  | 44/44 | 23/23 | 21/21 |
| france  | total  | 31/44 | 15/23 | 16/21 |
| france  | lyon   | 9/31  | 5/15  | 4/16  |

##### Group comparison - ColumnSet

This type of comparison is mainly used for what-if comparison but not limited to it. It involves the creation of a new
"virtual" column called `ColumnSet` that only exists in SquashQL to create groups among which the comparisons are
performed. Let's see a very simple example inspired from [https://www.causal.app/blog/everything-you-need-to-know-about-what-if-scenarios](https://www.causal.app/blog/everything-you-need-to-know-about-what-if-scenarios)

Our initial dataset looks like this
```
+----------+-----------+------------+-------------+
| scenario | saleprice | loavessold | pointofsale |
+----------+-----------+------------+-------------+
|     base |       2.0 |         80 |           B |
|     base |       2.0 |        100 |           A |
|       s1 |       3.0 |         50 |           B |
|       s1 |       3.0 |         74 |           A |
|       s2 |       4.0 |         20 |           B |
|       s2 |       4.0 |         55 |           A |
|       s3 |       2.0 |        100 |           A |
|       s3 |       3.0 |         50 |           B |
+----------+-----------+------------+-------------+
```

To compute the revenue
```typescript
import {ExpressionMeasure, from} from "@squashql/squashql-js"

const saleprice = new TableField("myTable.saleprice")
const loavessold = new TableField("myTable.loavessold")
const revenue = sum("revenue", saleprice.multiply(loavessold))
const query = from("myTable")
        .select([myTable.scenario], [], [revenue])
        .build()
```

Result
```
+----------+---------+
| scenario | revenue |
+----------+---------+
|     base |   360.0 |
|       s1 |   372.0 |
|       s2 |   300.0 |
|       s3 |   350.0 |
+----------+---------+
```

Let's say we want to compare each scenario s1, s2 and s3 with base plus each of these between them in the following order: s1 -> s2 -> s3.
To do that, we start by creating those groups that we put in a dedicated object

```typescript
import {
  GroupColumnSet
} from "@squashql/squashql-js"

const groups = new Map(Object.entries({
  "group1": ["base", "s1"],
  "group2": ["base", "s2"],
  "group3": ["base", "s3"],
  "group4": ["s1", "s2", "s3"],
}))
const columnSet = new GroupColumnSet(new TableField("group"), new TableField("myTable.scenario"), groups)
```

The first argument of `GroupColumnSet` is the name of the new (virtual) column that will be created.
The second argument is the name of the existing column whose values will be grouped together.
The third argument is the defined groups to be used for the comparison. The orders of the keys (group1, group2....)
and in the arrays are important.

We can use the `GroupColumnSet` as follows, as the second argument of `select`
```typescript
import {GroupColumnSet, ExpressionMeasure, from} from "@squashql/squashql-js"

const values = new Map(Object.entries({
  "group1": ["base", "s1"],
  "group2": ["base", "s2"],
  "group3": ["base", "s3"],
  "group4": ["s1", "s2", "s3"],
}))
const saleprice = new TableField("myTable.saleprice")
const loavessold = new TableField("myTable.loavessold")
const revenue = sum("revenue", saleprice.multiply(loavessold))
const columnSet = new GroupColumnSet(new TableField("group"), new TableField("myTable.scenario"), groups)
const query = from("myTable")
        .select([], [columnSet], [revenue])
        .build()
```

Result
```
+--------+----------+---------+
|  group | scenario | revenue |
+--------+----------+---------+
| group1 |     base |   360.0 |
| group1 |       s1 |   372.0 |
| group2 |     base |   360.0 |
| group2 |       s2 |   300.0 |
| group3 |     base |   360.0 |
| group3 |       s3 |   350.0 |
| group4 |       s1 |   372.0 |
| group4 |       s2 |   300.0 |
| group4 |       s3 |   350.0 |
+--------+----------+---------+
```
As you can see, adding the column set to the query will add two columns in the result: group and scenario so that we 
can remove scenario column from the first argument.

Now to perform the comparison, use the built-in measure `comparisonMeasureWithinSameGroup`
```typescript
import {GroupColumnSet, comparisonMeasureWithinSameGroup, ComparisonMethod, ExpressionMeasure, from} from "@squashql/squashql-js"

const values = new Map(Object.entries({
  "group1": ["base", "s1"],
  "group2": ["base", "s2"],
  "group3": ["base", "s3"],
  "group4": ["s1", "s2", "s3"],
}))
const saleprice = new TableField("myTable.saleprice")
const loavessold = new TableField("myTable.loavessold")
const revenue = sum("revenue", saleprice.multiply(loavessold))
const scenario = new TableField("myTable.scenario")
const columnSet = new GroupColumnSet(new TableField("group"), scenario, groups)
const revenueComparison = comparisonMeasureWithinSameGroup("revenueComparison",
        ComparisonMethod.ABSOLUTE_DIFFERENCE,
        revenue,
        new Map([[scenario, "s-1"]]))
const query = from("myTable")
        .select([], [columnSet], [revenue, revenueComparison])
        .build()
```

`{scenario: "s-1"}`, the "translation" or "shift" operator indicates that each value is to be compared with the one for 
the previous scenario (in the current group).
This is why order in `values` is important. Usage of `first` keyword is also possible (see time-series comparison).

Result
```
+--------+----------+---------+-------------------+
|  group | scenario | revenue | revenueComparison |
+--------+----------+---------+-------------------+
| group1 |     base |   360.0 |               0.0 |
| group1 |       s1 |   372.0 |              12.0 |
| group2 |     base |   360.0 |               0.0 |
| group2 |       s2 |   300.0 |             -60.0 |
| group3 |     base |   360.0 |               0.0 |
| group3 |       s3 |   350.0 |             -10.0 |
| group4 |     base |   360.0 |               0.0 |
| group4 |       s1 |   372.0 |              12.0 |
| group4 |       s2 |   300.0 |             -72.0 |
| group4 |       s3 |   350.0 |              50.0 |
+--------+----------+---------+-------------------+
```

##### Dimension comparison

It is also possible to perform a dimension comparison. Let's say we only want to compare s1, s2 and s3 belonging to the 
dimension scenario. To compare each value with the previous one:
```typescript
const revenueComparison = comparisonMeasureWithinSameGroup("revenueComparison",
        ComparisonMethod.ABSOLUTE_DIFFERENCE,
        revenue,
        new Map([[scenario, "s-1"]]))
const query = from("myTable")
        .select([scenario], [], [revenue, revenueComparison])
        .build()
```

Result
```
+----------+---------+-------------------+
| scenario | revenue | revenueComparison |
+----------+---------+-------------------+
|       s1 |   372.0 |               0.0 |
|       s2 |   300.0 |             -72.0 |
|       s3 |   350.0 |              50.0 |
+----------+---------+-------------------+
```

Replace `s-1` with `first` to compare s2 with s1 and s3 with s1 (first element of the dimension).

The order in which the comparison is performed is following the natural order of the values. If you want to change the order, 
you can pass a list of values:

```typescript
const elements = ["s3", "s1", "s2"]
const revenueComparison = comparisonMeasureWithinSameGroupInOrder("revenueComparison",
        ComparisonMethod.ABSOLUTE_DIFFERENCE,
        revenue,
        new Map([[scenario, "s-1"]]),
        elements)
const query = from("myTable")
        .select([scenario], [], [revenue, revenueComparison])
        .orderByFirstElements(scenario, elements) // orderByFirstElements to display correctly the elements but this is not mandatory
        .build()
```

Result
```
+----------+---------+-------------------+
| scenario | revenue | revenueComparison |
+----------+---------+-------------------+
|       s3 |   350.0 |               0.0 |
|       s1 |   372.0 |              22.0 |
|       s2 |   300.0 |             -72.0 |
+----------+---------+-------------------+
```

## Parameters

Once built, a query can accept additional parameters to change the way the query will be executed. Here's 
below the list of parameters that exist in SquashQL.

```typescript
const query = from("table")
        .select(...)
        .build()
```

### Query cache

Usage:
```typescript
const parameter = new QueryCacheParameter(Action.USE)
query.withParameter(parameter)
```

The query cache parameter can have three values.

```typescript
const parameter = new QueryCacheParameter(Action.USE)
```
`USE`: This is the default value when the parameter is not set in the query object. In that case
the [query cache](../README.md#under-the-hood) is used when executing the query. Intermediate results are retrieved
from the cache if they exist. If not, they are put in the cache when returned from the database.

```typescript
const parameter = new QueryCacheParameter(Action.NOT_USE)
```
`NOT_USE`: The cache won't be used. Intermediate results are retrieved from the database, and they are not put in the cache. 

```typescript
const parameter = new QueryCacheParameter(Action.INVALIDATE)
```
`INVALIDATE`: The cache is cleared. Intermediate results are retrieved from the database, and they are put in the cache. 




