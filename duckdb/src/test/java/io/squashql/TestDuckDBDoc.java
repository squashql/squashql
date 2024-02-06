package io.squashql;

import io.squashql.jackson.JacksonUtil;
import io.squashql.query.*;
import io.squashql.query.builder.Query;
import io.squashql.query.database.DuckDBQueryEngine;
import io.squashql.query.dto.*;
import io.squashql.table.PivotTable;
import io.squashql.table.PivotTableUtils;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.squashql.query.ComparisonMethod.ABSOLUTE_DIFFERENCE;
import static io.squashql.query.ComparisonMethod.DIVIDE;
import static io.squashql.query.Functions.*;
import static io.squashql.query.TableField.tableField;

/**
 * Various tests that print out tables used for documentation purpose. It does not test anything.
 */
public class TestDuckDBDoc {

  private final DuckDBDatastore ds = new DuckDBDatastore();
  private final DuckDBQueryEngine engine = new DuckDBQueryEngine(this.ds);
  private final QueryExecutor executor = new QueryExecutor(this.engine);

  /**
   * -- Create a sample employees table
   * CREATE TABLE employees (
   * employee_id INT PRIMARY KEY,
   * employee_name VARCHAR(255),
   * salary DECIMAL(10, 2),
   * age INT
   * );
   * -- Insert some sample data
   * INSERT INTO employees (employee_id, employee_name, salary, age)
   * VALUES
   * (1, 'John Doe', 50000.00, 50),
   * (2, 'Jane Smith', 65000.00, 33),
   * (3, 'Bob Johnson', 80000.00, 42),
   * (4, 'Alice Brown', 40000.00, 21),
   * (5, 'Peter Parker', 61000.00, 55),
   * (6, 'Jack Black', 73000.00, 39),
   * (7, 'Nicole Williams', 44000.00, 25),
   * (8, 'Charlie Wilson', 72000.00, 28);
   */
  @Test
  void bucketing() {
    String sql = """
            -- Create a sample employees table
            CREATE TABLE employees (
            employee_id INT PRIMARY KEY,
            employee_name VARCHAR(255),
            salary DECIMAL(10, 2),
            age INT
            );
            -- Insert some sample data
            INSERT INTO employees (employee_id, employee_name, salary, age)
            VALUES
               (1, 'John Doe', 50000.00, 50),
               (2, 'Jane Smith', 65000.00, 33),
               (3, 'Bob Johnson', 80000.00, 42),
               (4, 'Alice Brown', 40000.00, 21),
               (5, 'Peter Parker', 61000.00, 55),
               (6, 'Jack Black', 73000.00, 39),
               (7, 'Nicole Williams', 44000.00, 25),
               (8, 'Charlie Wilson', 72000.00, 28);
            """;
    this.engine.executeSql(sql);
    this.executor.executeRaw("select * from employees").show();

    VirtualTableDto salaryRangeTable = new VirtualTableDto("salaryRange",
            List.of("min", "max", "label"),
            List.of(
                    List.of(40_000, 50_000, "40k-50k"),
                    List.of(50_000, 60_000, "50k-60k"),
                    List.of(60_000, 70_000, "60k-70k"),
                    List.of(70_000, 80_000, "70k-80k"),
                    List.of(80_000, 1_000_000, "80k+")
            ));

    {
      QueryDto query = Query.from("employees")
              .join(salaryRangeTable, JoinType.INNER)
              .on(all(
                      criterion("employees.salary", "salaryRange.min", ConditionType.GE),
                      criterion("employees.salary", "salaryRange.max", ConditionType.LT)))
              .select(List.of(new TableField("salaryRange.label")), List.of(CountMeasure.INSTANCE))
              .build();
      this.executor.executeQuery(query).show();
    }

    VirtualTableDto ageRangeTable = new VirtualTableDto("ageRange",
            List.of("min", "max", "label"),
            List.of(
                    List.of(0, 30, "< 30"),
                    List.of(30, 50, "30 - 50"),
                    List.of(50, 100, "> 50")
            ));
    {
      QueryDto query = Query.from("employees")
              .join(salaryRangeTable, JoinType.INNER)
              .on(all(
                      criterion("employees.salary", "salaryRange.min", ConditionType.GE),
                      criterion("employees.salary", "salaryRange.max", ConditionType.LT)))
              .join(ageRangeTable, JoinType.INNER)
              .on(all(
                      criterion("employees.age", "ageRange.min", ConditionType.GE),
                      criterion("employees.age", "ageRange.max", ConditionType.LT)))
              .select(List.of(new TableField("salaryRange.label"), new TableField("ageRange.label")), List.of(CountMeasure.INSTANCE))
              .build();
      PivotTable pt = this.executor.executePivotQuery(new PivotTableQueryDto(
              query,
              List.of(new TableField("salaryRange.label")),
              List.of(new TableField("ageRange.label")),
              false));
      toJson(pt);
    }
  }

  @Test
  void pivotTable() {
    String sql = """
            -- Create a sales table
            CREATE TABLE sales (
                sale_id INT PRIMARY KEY,
                product VARCHAR(255) NOT NULL,
                region VARCHAR(255) NOT NULL,
                month VARCHAR(10) NOT NULL,
                revenue DECIMAL(10, 2) NOT NULL
            );

            -- Insert sample data
            INSERT INTO sales (sale_id, product, region, month, revenue) VALUES
            (1, 'ProductA', 'North', 'Jan', 1000.00),
            (2, 'ProductA', 'South', 'Jan', 1200.00),
            (3, 'ProductB', 'North', 'Jan', 800.00),
            (4, 'ProductB', 'South', 'Jan', 1500.00),
            (5, 'ProductA', 'North', 'Feb', 900.00),
            (6, 'ProductA', 'South', 'Feb', 1100.00),
            (7, 'ProductB', 'North', 'Feb', 750.00),
            (8, 'ProductB', 'South', 'Feb', 1300.00),
            (9, 'ProductA', 'North', 'Mar', 950.00),
            (10, 'ProductA', 'South', 'Mar', 1300.00),
            (11, 'ProductB', 'North', 'Mar', 700.00),
            (12, 'ProductB', 'South', 'Mar', 1400.00),
            (13, 'ProductC', 'North', 'Jan', 1200.00),
            (14, 'ProductC', 'South', 'Jan', 1000.00),
            (15, 'ProductC', 'North', 'Feb', 1100.00);
                        """;

    this.engine.executeSql(sql);
    this.executor.executeRaw("select * from sales").show();

    QueryDto query = Query.from("sales")
            .select(List.of(new TableField("product"), new TableField("region"), new TableField("month")), List.of(sum("sales", "revenue")))
            .orderBy(new TableField("month"), List.of("Jan", "Feb", "Mar"))
            .orderBy(new TableField("product"), OrderKeywordDto.ASC)
            .orderBy(new TableField("region"), OrderKeywordDto.ASC)
            .build();
    PivotTable pt = this.executor.executePivotQuery(new PivotTableQueryDto(
            query,
            List.of(new TableField("product"), new TableField("region")),
            List.of(new TableField("month")),
            false));
    pt.show();
    toJson(pt);
  }

  @Test
  void parentChildComparison() {
    String sql = """
            -- Create Continents Table
            CREATE TABLE continents (
                continent_id INT PRIMARY KEY,
                continent_name VARCHAR(50)
            );

            -- Insert data into Continents Table
            INSERT INTO continents (continent_id, continent_name) VALUES
            (2, 'Europe'),
            (3, 'Asia'),
            (5, 'Africa');

            -- Create Countries Table
            CREATE TABLE countries (
                country_id INT PRIMARY KEY,
                country_name VARCHAR(50),
                continent_id INT
            );

            -- Insert data into Countries Table
            INSERT INTO countries (country_id, country_name, continent_id) VALUES
            (101, 'USA', 1),
            (102, 'Canada', 1),
            (103, 'Germany', 2),
            (104, 'France', 2),
            (105, 'China', 3),
            (106, 'India', 3),
            (107, 'Brazil', 4),
            (108, 'Argentina', 4),
            (109, 'Nigeria', 5),
            (110, 'South Africa', 5);

            -- Create Cities Table
            CREATE TABLE cities (
                city_id INT PRIMARY KEY,
                city_name VARCHAR(50),
                country_id INT,
                sales_amount DECIMAL(10, 2)
            );

            -- Insert data into Cities Table
            INSERT INTO cities (city_id, city_name, country_id, sales_amount) VALUES
            (1001, 'New York', 101, 15000.00),
            (1002, 'Los Angeles', 101, 12000.50),
            (1003, 'Toronto', 102, 10000.75),
            (1004, 'Berlin', 103, 8000.25),
            (1005, 'Paris', 104, 9500.50),
            (1006, 'Beijing', 105, 12000.00),
            (1007, 'Mumbai', 106, 11000.75),
            (1008, 'Sao Paulo', 107, 13000.25),
            (1009, 'Buenos Aires', 108, 11500.50),
            (1010, 'Lagos', 109, 9000.00),
            (1011, 'Johannesburg', 110, 8500.75),
            (1012, 'Chicago', 101, 12500.00),
            (1013, 'Hamburg', 103, 8500.50),
            (1014, 'Shanghai', 105, 11000.25),
            (1015, 'Delhi', 106, 10500.75),
            (1016, 'Rio de Janeiro', 107, 12000.00),
            (1017, 'Cape Town', 110, 9500.50),
            (1018, 'Toronto', 102, 9800.25),
            (1019, 'Munich', 103, 8800.50),
            (1020, 'Lyon', 104, 9200.25);
                        """;

    this.engine.executeSql(sql);
    this.executor.executeRaw("select * from cities").show();
    this.executor.executeRaw("select * from cities inner join countries on cities.country_id = countries.country_id inner join continents on countries.continent_id = continents.continent_id").show();

    List<Field> fields = List.of(new TableField("continent_name"), new TableField("country_name"), new TableField("city_name"));
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", DIVIDE, sum("sales", "sales_amount"), fields);

    QueryDto query = Query.from("cities")
            .join("countries", JoinType.INNER)
            .on(criterion(new TableField("cities.country_id"), new TableField("countries.country_id"), ConditionType.EQ))
            .join("continents", JoinType.INNER)
            .on(criterion(new TableField("countries.continent_id"), new TableField("continents.continent_id"), ConditionType.EQ))
            .select(fields, List.of(sum("sales", "sales_amount"), Functions.multiply("sales ratio %", pOp, Functions.integer(100))))
            .build();
    PivotTable pt = this.executor.executePivotQuery(new PivotTableQueryDto(
            query,
            fields,
            List.of(),
            false));
    pt.show();
    toJson(pt);
  }

  @Test
  void timeComparison() {
    String sql = """
            CREATE TABLE sales (
                sale_id INT PRIMARY KEY,
                year INT,
                month INT,
                amount DECIMAL(10, 2)
            );

            INSERT INTO sales (sale_id, year, month, amount) VALUES
            (8, 2022, 8, 3200.25),
            (9, 2022, 9, 3500.50),
            (10, 2022, 10, 3800.75),
            (11, 2022, 11, 4000.00),
            (12, 2022, 12, 4200.25),
            (13, 2023, 1, 4500.50),
            (14, 2023, 2, 4800.75),
            (15, 2023, 3, 5000.00),
                        """;

    this.engine.executeSql(sql);
    this.executor.executeRaw("select * from sales").show();

    List<Field> fields = List.of(new TableField("year"), new TableField("month"));

    Period.Month period = new Period.Month(tableField("month"), tableField("year"));
    AggregatedMeasure sales = new AggregatedMeasure("sales", "amount", "sum");
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "Month-over-Month (MoM) Growth",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of(period.month(), "m-1", period.year(), "y"),
            period);

    QueryDto query = Query.from("sales")
            .select(fields, List.of(sales, m))
            .build();
    PivotTable pt = this.executor.executePivotQuery(new PivotTableQueryDto(
            query,
            fields,
            List.of(),
            false));
    pt.show();
    toJson(pt);
  }

  /**
   * Json output for jsfiddle. Example: <a href="https://jsfiddle.net/azeq/dqebkp2x/">https://jsfiddle.net/azeq/dqebkp2x/</a>
   */
  private static void toJson(PivotTable pt) {
    List<Map<String, Object>> cells = PivotTableUtils.generateCells(pt);
    System.out.println(JacksonUtil.serialize(new PivotTableQueryResultDto(cells, pt.rows, pt.columns, pt.values)));
  }
}
