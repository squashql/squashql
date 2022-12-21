## Summary

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
} from "@aitm1/aitm-js"

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
} from "@aitm1/aitm-js"

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
SELECT col1, col2, sum(col3) as alias1, sum(col4) as alias2
FROM myTable
GROUP BY col1, col2
```

## Filtering

Queries can be filtered by using Criteria class. A Criteria instance can contain a condition on a single field and can be build as so:
```typescript
import { criterion } from "@aitm1/aitm-js"
const criteria = criterion("col2", eq("c"));
```

Several criteria can be chained with AND or OR by using the methods `any` and `all`

```typescript
import {
  from, sum, avg, _in, eq, criterion, all
} from "@aitm1/aitm-js"

const q = from("myTable")
        .where(all([criterion("col1", _in(["a", "b"])), criterion("col2", eq("c"))]))
        .select(
                ["col1", "col2"],
                [],
                [sum("alias1", "col3"), avg("alias2", "col4")])
        .build();
```

```sql
SELECT col1, col2, sum(col3) as alias1, sum(col4) as alias2
FROM myTable
WHERE (col1 IN ('a', 'b')
  AND col2 = 'c')
GROUP BY col1, col2
```

Condition operators available: `eq, neq, lt, le, gt, ge, _in, isNull, isNotNull, like, and, or`.

## Joining Tables

Tables can be joined with other tables by using `innerJoin` and `leftOuterJoin` immediately followed by `on` operator (
equivalent to `ON` clause in SQL)

### Single join / Single join condition

```typescript
import {
  from
} from "@aitm1/aitm-js"

const q = from("myTable")
        .innerJoin("refTable")
        .on("myTable", "id", "refTable", "id")
        .select(["myTable.col", "refTable.col"], [], [])
        .build()
```

```sql
SELECT myTable.col, refTable.col
FROM myTable
       INNER JOIN refTable ON myTable.id = refTable.id
```

### Single join / Multiple join condition

```typescript
import {
  from
} from "@aitm1/aitm-js"

const q = from("myTable")
        .innerJoin("refTable")
        .on("myTable", "id1", "refTable", "id1")
        .on("myTable", "id2", "refTable", "id2")
        .select(["myTable.col", "refTable.col"], [], [])
        .build()
```

```sql
SELECT myTable.col, refTable.col
FROM myTable
       INNER JOIN refTable ON myTable.id1 = refTable.id1 AND myTable.id2 = refTable.id2 
```

### Multiple join

```typescript
import {
  from
} from "@aitm1/aitm-js"

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
SELECT myTable.col, refTable.col
FROM myTable
       INNER JOIN refTable ON myTable.id = refTable.id
       LEFT OUTER JOIN otherTable ON myTable.id = otherTable.key1 AND refTable.id = otherTable.key2
```

## Rollup

> The `ROLLUP` option allows you to include extra rows that represent the subtotals, which are commonly referred to as super-aggregate rows,
> along with the grand total row. By using the `ROLLUP` option, you can use a single query to generate multiple grouping sets.
(source https://www.sqltutorial.org/sql-rollup/)

```typescript
import {
  from, sum
} from "@aitm1/aitm-js"

const query = from("populationTable")
        .select(["continent", "country", "city"], [], [sum("pop", "population")])
        .rollup(["continent", "country", "city"])
        .build();
```

```sql
SELECT continent,
       country,
       city,
       sum(population) as population
FROM populationTable
GROUP BY ROLLUP (continent, country, city);
```

Example
```
+-------------+---------+----------+------------+
|   continent | country |     city |        pop |
+-------------+---------+----------+------------+
| Grand Total |    null |     null |       28.5 |
|          am |   Total |     null |       17.0 |
|          am |  canada |    Total |        6.0 |
|          am |  canada | montreal |        2.0 |
|          am |  canada |    otawa |        1.0 |
|          am |  canada |  toronto |        3.0 |
|          am |     usa |    Total |       11.0 |
|          am |     usa |  chicago |        3.0 |
|          am |     usa |      nyc |        8.0 |
|          eu |   Total |     null |       11.5 |
|          eu |  france |    Total |        2.5 |
|          eu |  france |     lyon |        0.5 |
|          eu |  france |    paris |        2.0 |
|          eu |      uk |    Total |        9.0 |
|          eu |      uk |   london |        9.0 |
+-------------+---------+----------+------------+
```

## Subqueries in FROM Clause (also known as inner or nested queries)

A subquery can be nested in the `FROM` clause. Start by using `fromSubQuery` instead of 'from'

```typescript
import {
  from, fromSubQuery, sum, avg,
} from "@aitm1/aitm-js"

const subQuery = from("student")
        .select(["name"], [], [sum("score_sum", "score")])
        .build()

const query = fromSubQuery(subQuery)
        .select([], [], [avg("result", "score_sum")])
        .build()
```

Example: Return the average total for all students

```sql
SELECT AVG(score_sum) AS result
FROM (SELECT SUM(score) AS score_sum FROM student GROUP BY name);
```

(from [https://mariadb.com/kb/en/subqueries-in-a-from-clause/](https://mariadb.com/kb/en/subqueries-in-a-from-clause/))

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

An aggregate measure is computed by applying an aggregation function over a list of field values such as avg, count, sum, min, max...

Aggregation can also be applied to only the rows matching a [condition](#filtering) with `sumIf`, `countIf`...

```typescript
import {
  sum,
  avg,
  sumIf,
  eq,
  criterion,      
} from "@aitm1/aitm-js"

const amountSum = sum("sum_amount", "amount")
const amountAvg = avg("avg_amount", "amount")
const sales = sumIf("sales", "amount", criterion("IncomeExpense", eq("Revenue")))

const query = from("myTable")
        .select([], [], [amountSum, amountAvg, sales])
        .build()
```

```sql
SELECT SUM(amount)                                                AS sum_amount,
       AVG(amount)                                                AS avg_amount,
       SUM(CASE WHEN IncomeExpense = 'Revenue' THEN amount 0 END) AS sales
FROM myTable;
```

#### Expression measure

An expression measure is a measure that accepts a raw sql expression as argument.

```typescript
import {
  ExpressionMeasure
} from "@aitm1/aitm-js"

const expression = new ExpressionMeasure("myMeasure", "sum(price * quantity)")
const query = from("myTable")
        .select([], [], [expression])
        .build()
```

```sql
SELECT SUM(price * quantity) AS myMeasure FROM myTable;
```

### Calculated measure

Unlike a basic measure, a calculated measure is computed by AITM (not the database) by fetching all the required values
from the underlying database before applying the defined calculation.
It is defined as the combination of other measures that can be either basic or not.

#### Elementary: addition, subtraction, multiplication and division

```typescript
import {
  sum,
  multiply, divide, plus, minus
} from "@aitm1/aitm-js"

const aSum = sum("aSum", "a")
const square = multiply("square", aSum, aSum)
const twoTimes = plus("twoTimes", aSum, aSum)
const zero = minus("zero", aSum, aSum)
const one = divide("one", aSum, aSum)
```

Constant measures can be defined with `decimal` or `integer` operators:

```typescript
import {
  sum,
  decimal
} from "@aitm1/aitm-js"

const a = sum("aSum", "a")
const b = sum("bSum", "b")
const ratio = divide("ratio", a, b)
const percent = multiply("percent", ratio, decimal(100)) 
```

#### Constant measure

Used to define measure with a constant value in order to combine it with other measures. See below.

```typescript
import {
  decimal, integer
} from "@aitm1/aitm-js"

const oneHundredDecimal = decimal(100)
const oneHundredInteger = integer(100)
```

#### Complex: comparison

##### Time-series comparison

Comparison between "time" period like year, semester, quarter, month.

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
} from "@aitm1/aitm-js"

const scoreSum = sum("score_sum", "score")
const comparisonScore = comparisonMeasureWithPeriod(
        "compare with previous year",
        ComparisonMethod.ABSOLUTE_DIFFERENCE,
        scoreSum,
        new Map(Object.entries({"semester": "s-1"})),
        new Semester("semester", "year"))

const query = from("student")
        .select(["year", "semester", "name"], [], [scoreSum, comparisonScore])
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
remove 1'.
The `Semester` object is meant to make AITM understand the measure is working with a **time period** so that it knows
that
the previous semester value of the 1st semester of 2023 is the 2nd semester of 2022.

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
} from "@aitm1/aitm-js"

const scoreSum = sum("score_sum", "score")
const comparisonScore = comparisonMeasureWithPeriod(
        "compare with previous year",
        ComparisonMethod.RELATIVE_DIFFERENCE,
        scoreSum,
        new Map(Object.entries({"year": "y-1"})),
        new Year("year"))

const query = from("student")
        .select(["year", "name"], [], [scoreSum, multiply("progression in %", comparisonScore, decimal(100))])
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

AITM introduces the concept of organizing hierarchically several columns in order to compare aggregates and sub-aggregates
compute at different levels of the lineage.

Example: compute the ratio of population of a city to its country and of a country to its continent. 
```
+-------------+---------+----------+------------+
|   continent | country |     city |        pop |
+-------------+---------+----------+------------+
| Grand Total |    null |     null |       28.5 |
|          am |   Total |     null |       17.0 |
|          am |  canada |    Total |        6.0 |
|          am |  canada | montreal |        2.0 |
|          am |  canada |    otawa |        1.0 |
|          am |  canada |  toronto |        3.0 |
|          am |     usa |    Total |       11.0 |
|          am |     usa |  chicago |        3.0 |
|          am |     usa |      nyc |        8.0 |
|          eu |   Total |     null |       11.5 |
|          eu |  france |    Total |        2.5 |
|          eu |  france |     lyon |        0.5 |
|          eu |  france |    paris |        2.0 |
|          eu |      uk |    Total |        9.0 |
|          eu |      uk |   london |        9.0 |
+-------------+---------+----------+------------+
```

```typescript
import {
  ComparisonMethod,
  from,
  sum,
  comparisonMeasureWithParent,
} from "@aitm1/aitm-js"

const pop = sum("pop", "population")
const ancestors = ["city", "country", "continent"]
const ratio = comparisonMeasureWithParent("ratio", ComparisonMethod.DIVIDE, pop, ancestors)
const query = from("populationTable")
        .select(["continent", "country", "city"], [], [pop, ratio])
        .rollup(["continent", "country", "city"])
        .build()
```

`comparisonMeasureWithParent` method is used to create a special measure built to compare values of an underlying
measure (third argument) with the parent values of the same measure. Parenthood is indicated with the array of `ancestors`
(fourth argument) which contains column names in "lineage order". 

Note the columns used to define the ancestors need to be passed to the select method but not necessary to the rollup method. 

Result
```
+-------------+---------+----------+------------+---------------------+
|   continent | country |     city |        pop |               ratio |
+-------------+---------+----------+------------+---------------------+
| Grand Total |    null |     null |       28.5 |                 1.0 |
|          am |   Total |     null |       17.0 |  0.5964912280701754 |
|          am |  canada |    Total |        6.0 | 0.35294117647058826 |
|          am |  canada | montreal |        2.0 |  0.3333333333333333 |
|          am |  canada |    otawa |        1.0 | 0.16666666666666666 |
|          am |  canada |  toronto |        3.0 |                 0.5 |
|          am |     usa |    Total |       11.0 |  0.6470588235294118 |
|          am |     usa |  chicago |        3.0 |  0.2727272727272727 |
|          am |     usa |      nyc |        8.0 |  0.7272727272727273 |
|          eu |   Total |     null |       11.5 | 0.40350877192982454 |
|          eu |  france |    Total |        2.5 | 0.21739130434782608 |
|          eu |  france |     lyon |        0.5 |                 0.2 |
|          eu |  france |    paris |        2.0 |                 0.8 |
|          eu |      uk |    Total |        9.0 |   0.782608695652174 |
|          eu |      uk |   london |        9.0 |                 1.0 |
+-------------+---------+----------+------------+---------------------+
```

##### Dynamic comparison - What-if - ColumnSet

This type of comparison is mainly used for what-if comparison but not limited to it. It involves the creation of a new
"virtual" column called `ColumnSet` that only exists in AITM to create groups among which the comparisons are
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
import {ExpressionMeasure, from} from "@aitm1/aitm-js"

const revenue = new ExpressionMeasure("revenue", "sum(saleprice * loavessold)")
const query = from("myTable")
        .select(["scenario"], [], [revenue])
        .build();
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
  BucketColumnSet
} from "@aitm1/aitm-js"

const groups = new Map(Object.entries({
  "group1": ["base", "s1"],
  "group2": ["base", "s2"],
  "group3": ["base", "s3"],
  "group4": ["s1", "s2", "s3"],
}))
const columnSet = new BucketColumnSet("group", "scenario", values);
```

The first argument of `BucketColumnSet` is the name of the new (virtual) column that will be created.
The second argument is the name of the existing column whose values will be grouped together.
The third argument is the defined groups to be used for the comparison. The orders of the keys (group1, group2....)
and in the arrays are important.

We can use the `BucketColumnSet` as follows
```typescript
import {BucketColumnSet, ExpressionMeasure, from} from "@aitm1/aitm-js"

const values = new Map(Object.entries({
  "group1": ["base", "s1"],
  "group2": ["base", "s2"],
  "group3": ["base", "s3"],
  "group4": ["s1", "s2", "s3"],
}))
const columnSet = new BucketColumnSet("group", "scenario", values);
const revenue = new ExpressionMeasure("revenue", "sum(saleprice * loavessold)")
const query = from("myTable")
        .select([], [columnSet], [revenue])
        .build();
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

Now to perform the comparison, use the built-in measure `comparisonMeasureWithBucket`
```typescript
import {BucketColumnSet, comparisonMeasureWithBucket, ComparisonMethod, ExpressionMeasure, from} from "@aitm1/aitm-js"

const values = new Map(Object.entries({
  "group1": ["base", "s1"],
  "group2": ["base", "s2"],
  "group3": ["base", "s3"],
  "group4": ["s1", "s2", "s3"],
}))
const columnSet = new BucketColumnSet("group", "scenario", values);
const revenue = new ExpressionMeasure("revenue", "sum(saleprice * loavessold)")
const revenueComparison = comparisonMeasureWithBucket("revenueComparison",
        ComparisonMethod.ABSOLUTE_DIFFERENCE,
        revenue,
        new Map(Object.entries({"scenario": "s-1"})),)
const query = from("myTable")
        .select([], [columnSet], [revenue, revenueComparison])
        .build();
```

`{"scenario": "s-1"}` indicates that each value is to be compared with the one for the previous scenario (in the current group).
This is why order in `values` is important.

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
