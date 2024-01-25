## 💡Key features

### Pivot Table

Create pivot table in SQL with a simple and intuitive syntax. Totals and subtotals **are computed by the database**.

```typescript
const pivotConfig: PivotConfig = {
  rows: [sales.product, sales.region],
  columns: [sales.month]
}
const query = from(sales._name)
        .select([sales.product, sales.region, sales.month], [], [sum("sales", sales.revenue)])
        .build()
querier.executePivotQuery(query, pivotConfig)
```

![pivot-table-sales.png](documentation%2Fassets%2Fpivot-table-sales.png)
👉 https://jsfiddle.net/azeq/c6f9ox4u/

Try the [tutorial](https://github.com/squashql/squashql-showcase/blob/main/TUTORIAL.md#pivot-table) to build your own pivot table.

### Bucketing

Bucketing, also known as binning or discretization, is a technique used in data analysis to group continuous or numerical 
data into discrete intervals or "buckets." 

<details><summary>SQL</summary>

```sql
-- Create a sample employees table
CREATE TABLE employees
(
  employee_id   INT PRIMARY KEY,
  employee_name VARCHAR(255),
  salary        DECIMAL(10, 2),
  age           INT
);
-- Insert some sample data
INSERT INTO employees (employee_id, employee_name, salary, age)
VALUES (1, 'John Doe', 50000.00, 50),
       (2, 'Jane Smith', 65000.00, 33),
       (3, 'Bob Johnson', 80000.00, 42),
       (4, 'Alice Brown', 40000.00, 21),
       (5, 'Peter Parker', 61000.00, 55),
       (6, 'Jack Black', 73000.00, 39),
       (7, 'Nicole Williams', 44000.00, 25),
       (8, 'Charlie Wilson', 72000.00, 28);
```

</details>

```
+-------------+-----------------+---------+-----+
| employee_id |   employee_name |  salary | age |
+-------------+-----------------+---------+-----+
|           1 |        John Doe | 50000.0 |  50 |
|           2 |      Jane Smith | 65000.0 |  33 |
|           3 |     Bob Johnson | 80000.0 |  42 |
|           4 |     Alice Brown | 40000.0 |  21 |
|           5 |    Peter Parker | 61000.0 |  55 |
|           6 |      Jack Black | 73000.0 |  39 |
|           7 | Nicole Williams | 44000.0 |  25 |
|           8 |  Charlie Wilson | 72000.0 |  28 |
+-------------+-----------------+---------+-----+
```

Easy to bucket on one or several attributes. 
Result can be displayed in a [pivot table](https://github.com/squashql/squashql-showcase/blob/main/TUTORIAL.md#pivot-table)
to enhance data visualization and help analysis.

![bucketing-age-salary.png](documentation/assets/bucketing-age-salary.png)
👉 https://jsfiddle.net/azeq/r1xfqt89/

[More](https://github.com/squashql/squashql/blob/main/documentation/QUERY.md#joining-on-virtual-created-on-the-fly-at-query-time)

### Comparison measures

Make calculations that are not possible or cumbersome in SQL easy to perform.

[More](documentation/QUERY.md#complex-comparison)

### Drilling across

Query two or more fact tables and stitch together the results on shared columns. 

```
Result query 1
+-------------+---------------+
|     product | quantity sold |
+-------------+---------------+
| Grand Total |            54 |
|           A |            15 |
|           B |            23 |
|           C |            16 |
+-------------+---------------+

Result query 2
+-------------+-------------------+
|     product | quantity returned |
+-------------+-------------------+
| Grand Total |                 5 |
|           A |                 1 |
|           C |                 3 |
|           D |                 1 |
+-------------+-------------------+

Drilling across result (with left join)
+-------------+---------------+-------------------+
|     product | quantity sold | quantity returned |
+-------------+---------------+-------------------+
| Grand Total |            54 |                 5 |
|           A |            15 |                 1 |
|           B |            23 |              null |
|           C |            16 |                 3 |
+-------------+---------------+-------------------+
```

[More](documentation/DRILLING-ACROSS.md)

### Query cache

SquashQL provides an in-memory query cache to not re-execute queries already executed.
Caching can be customized or deactivated.

[More](documentation/CACHE.md)

## 📕 Documentation

- [Typescript API](documentation/QUERY.md)
- [Drilling across](documentation/DRILLING-ACROSS.md)
- [Cache](documentation/CACHE.md)
