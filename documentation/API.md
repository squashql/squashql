# API

This is a quick introduction to SquashQL API. For an in-depth documentation, please refer to the [Typescript API Handbook](documentation/QUERY.md).

## Regular query

To execute a query. It accepts a json object built with the Typescript library and returns a JSON
object representing the result table of the computation. The object returns is of type [QueryResult](https://github.com/squashql/squashql/blob/main/js/typescript-library/src/querier.ts)

```typescript
class Budget {
  readonly _name: string = "budget"
  readonly incomeExpenditure: TableField = new TableField("budget.IncomeExpenditure")
  readonly amount: TableField = new TableField("budget.Amount")
  readonly year: TableField = new TableField("budget.Year")
  readonly month: TableField = new TableField("budget.Month")
  readonly category: TableField = new TableField("budget.Category")
}
const budget = new Budget()

const income = sumIf("Income", budget.amount, criterion(budget.incomeExpenditure, eq("Income")))
const expenditure = sumIf("Expenditure", budget.amount, criterion(budget.incomeExpenditure, neq("Income")))
const query = from(budget._name)
        .select([budget.year, budget.month, budget.category], [], [income, expenditure])
        .build()
querier.executeQuery(query).then(r => console.log(JSON.stringify(r)))
```

The `execute` method accepts two other arguments:
- `PivotConfig` (detailed [below](#pivot-table-query))
- `stringify` default value is false. If true, it returns the result as a string instead of a JSON object that can be printed
  to display the result table:

```typescript
querier.executeQuery(query, true).then(r => console.log(r))
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

## Pivot table query

SquashQL brings the ability to execute any SQL queries and transform their results into a format suitable for pivot table visualization. Check out [this example](https://github.com/squashql/squashql-showcase/blob/main/TUTORIAL.md#pivot-table) in our tutorial.

To execute a query whose result will be enriched with totals and subtotals to be able to display the result as a pivot table.
It accepts a json object built with the Typescript library and returns a JSON object representing the result table of the computation.
The object returns is of type [PivotTableQueryResult](https://github.com/squashql/squashql/blob/main/js/typescript-library/src/querier.ts).

To enable the pivot table feature, a `PivotConfig` parameter needs to be pass to the `execute` method:
```typescript
const pivotConfig: PivotConfig = {
  rows: [budget.category],
  columns: [budget.year, budget.month]
}
```

A third optional parameter `hiddenTotals?: Array<Field>` can be set in `PivotConfig` to indicate which totals should not 
be computed. See example below.

`PivotConfig` is used by SquashQL to know which totals and subtotals needs to be computed. The union of the two lists rows and columns
must be exactly equal to the list of columns provided in the select.

```typescript
const income = sumIf("Income", budget.amount, criterion(budget.incomeExpenditure, eq("Income")))
const expenditure = sumIf("Expenditure", budget.amount, criterion(budget.incomeExpenditure, neq("Income")))
const query = from(budget._name)
        .select([budget.year, budget.month, budget.category], [], [income, expenditure])
        .build()
querier.executePivotQuery(query, pivotConfig).then(r => console.log(JSON.stringify(r)))
```

With `stringify = true`
```typescript
querier.executePivotQuery(query, pivotConfig, true).then(r => console.log(r))
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

With `hiddenTotals` set
```typescript
const pivotConfig: PivotConfig = {
  rows: [budget.category],
  columns: [budget.year, budget.month],
  hiddenTotals: [budget.year]
}
```

```
+---------------------------------+--------+--------------------+--------+--------------------+--------+ ...
|                            Year |   2022 |               2022 |   2022 |               2022 |   2022 | ...
|                           Month |  Total |              Total |      1 |                  1 |      2 | ...
|                        Category | Income |        Expenditure | Income |        Expenditure | Income | ...
+---------------------------------+--------+--------------------+--------+--------------------+--------+ ...
|                     Grand Total | 5790.0 |  5599.740000000005 | 1930.0 | 1823.0399999999997 | 1930.0 | ...
|            Cigarettes & Alcohol |    NaN | 161.82000000000002 |    NaN | 54.870000000000005 |    NaN | ...
|                  Current Income | 5790.0 |                NaN | 1930.0 |                NaN | 1930.0 | ...
| Media & Clothes & Food Delivery |    NaN | 393.90999999999997 |    NaN |             118.75 |    NaN | ...
|             Minimum expenditure |    NaN |  4159.000000000002 |    NaN |            1383.87 |    NaN | ...
|                Outing Lifestyle |    NaN |              509.7 |    NaN |             141.38 |    NaN | ...
|             Sport & Game & misc |    NaN |             375.31 |    NaN | 124.17000000000002 |    NaN | ...
+---------------------------------+--------+--------------------+--------+--------------------+--------+ ...
```

## Drilling across query

To execute *Drilling across* query i.e. querying two fact tables. The two results are aligned by
performing a sort-merge operation on the common attribute column headers.
The object returns is of type [QueryResult](https://github.com/squashql/squashql/blob/main/js/typescript-library/src/querier.ts).

```typescript
class MyTable {
  readonly _name: string = "myTable"
  readonly id: TableField = new TableField("myTable.id")
  readonly col1: TableField = new TableField("myTable.col1")
}

class OtherTable {
  readonly _name: string = "otherTable"
  readonly id: TableField = new TableField("otherTable.id")
  readonly col1: TableField = new TableField("otherTable.col1")
  readonly col2: TableField = new TableField("otherTable.col2")
  readonly field: TableField = new TableField("otherTable.field")
}

const myTable = new MyTable()
const otherTable = new OtherTable()

const myFirstQuery = from(myTable._name)
        .select([myTable.col1], [], [count])
        .build()
const mySecondQuery = from(otherTable._name)
        .select([otherTable.col1, otherTable.col2], [], [sum("alias", otherTable.field)])
        .build()
querier.executeQuery(new QueryMerge(myFirstQuery, mySecondQuery)).then(response => console.log(response))
```

Note the result of `QueryMerge` can also be displayed as a pivot table by using `querier.executePivotQuery()`. 

Full documentation of [Drilling across in the dedicated page](./documentation/DRILLING-ACROSS.md).

## Minify

Minify is an option to remove columns that contain only null values from the final result. This attribute exists in the 
following classes:

- Query
- QueryMerge
- QueryJoin

If not set, the default value is `true`. You can change it like this:

```typescript
const q: Query | QueryMerge | QueryJoin = ...
q.minify = false
```

It is also useful when using `PivotTableQuery` and `PivotTableQueryMerge`. The attribute needs to be set to the desired value
on the underlying query object: `PivotTableQuery#query` or `PivotTableQueryMerge#query`.

## Under the hood

SquashQL helps you to execute multi-dimensional queries compatible with several databases. The syntax is closed to SQL but...

> What happens exactly when the query is sent to SquashQL?

Once the query is received by SquashQL server, it is analyzed and broken down into one or multiple *elementary*
queries that can be executed by the underlying database. Before sending those queries for execution, SquashQL first looks
into its query cache (see [CaffeineQueryCache](https://github.com/squashql/squashql/blob/main/core/src/main/java/io/squashql/query/CaffeineQueryCache.java))
to see if the result of each *elementary* query exist. If it does, the result is returned immediately. If it does not,
the elementary query is translated into compatible SQL statement, sent and executed by the database.
The intermediary results are cached into SquashQL query cache for future reuse and used to compute the final query result. 
