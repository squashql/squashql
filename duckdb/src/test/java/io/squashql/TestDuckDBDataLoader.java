package io.squashql;

import com.google.common.collect.ImmutableList;
import io.squashql.jackson.JacksonUtil;
import io.squashql.query.CountMeasure;
import io.squashql.query.Header;
import io.squashql.query.QueryExecutor;
import io.squashql.query.TableField;
import io.squashql.query.builder.Query;
import io.squashql.query.compiled.CompiledAggregatedMeasure;
import io.squashql.query.database.DuckDBQueryEngine;
import io.squashql.query.dto.*;
import io.squashql.table.ColumnarTable;
import io.squashql.table.PivotTable;
import io.squashql.table.Table;
import io.squashql.table.TableUtils;
import io.squashql.transaction.DuckDBDataLoader;
import io.squashql.type.AliasedTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.squashql.query.Functions.*;

public class TestDuckDBDataLoader {

  @Test
  void testCreateAndLoadTableFromTableObject() {
    Table table = new ColumnarTable(
            List.of(new Header("typology", String.class, false),
                    new Header("category", String.class, false),
                    new Header("price.avg", int.class, true)),
            Set.of(new CompiledAggregatedMeasure("price.avg", new AliasedTypedField("price"), "avg", null, false)),
            List.of(
                    new ArrayList<>(Arrays.asList("MN", "MN", "MDD", "MDD")),
                    new ArrayList<>(Arrays.asList("A", "B", "A", "C")),
                    new ArrayList<>(Arrays.asList(2, 3, 6, 5))));

    DuckDBDatastore ds = new DuckDBDatastore();
    DuckDBDataLoader loader = new DuckDBDataLoader(ds);
    loader.createOrReplaceTable("myTable", table);

    QueryExecutor executor = new QueryExecutor(new DuckDBQueryEngine(ds));
    Table result = executor.executeRaw("select * from myTable");
    Assertions.assertThat(result).containsExactly(
            List.of("MN", "A", 2),
            List.of("MN", "B", 3),
            List.of("MDD", "A", 6),
            List.of("MDD", "C", 5));
  }

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
  void name() {
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
    DuckDBDatastore ds = new DuckDBDatastore();
    DuckDBQueryEngine engine = new DuckDBQueryEngine(ds);
    engine.executeSql(sql);
    QueryExecutor queryExecutor = new QueryExecutor(engine);
    queryExecutor.executeRaw("select * from employees").show();

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
      queryExecutor.executeQuery(query).show();
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
      PivotTable pt = queryExecutor.executePivotQuery(new PivotTableQueryDto(
              query,
              List.of(new TableField("salaryRange.label")),
              List.of(new TableField("ageRange.label"))));
      jsonpt(pt);

    }
  }

  private static void jsonpt(PivotTable pt) {
    List<String> fields = pt.table.headers().stream().map(Header::name).collect(Collectors.toList());
    SimpleTableDto simpleTable = SimpleTableDto.builder()
            .rows(ImmutableList.copyOf(pt.table.iterator()))
            .columns(fields)
            .build();
    QueryResultDto result = QueryResultDto.builder()
            .table(simpleTable)
            .metadata(TableUtils.buildTableMetadata(pt.table))
            .build();
    System.out.println(JacksonUtil.serialize(new PivotTableQueryResultDto(result, pt.rows, pt.columns, pt.values)));
  }

  @Test
  void pt() {
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

    DuckDBDatastore ds = new DuckDBDatastore();
    DuckDBQueryEngine engine = new DuckDBQueryEngine(ds);
    engine.executeSql(sql);
    QueryExecutor queryExecutor = new QueryExecutor(engine);
    queryExecutor.executeRaw("select * from sales").show();

    QueryDto query = Query.from("sales")
            .select(List.of(new TableField("product"), new TableField("region"), new TableField("month")), List.of(sum("sales", "revenue")))
            .orderBy(new TableField("month"), List.of("Jan", "Feb", "Mar"))
            .orderBy(new TableField("product"), OrderKeywordDto.ASC)
            .orderBy(new TableField("region"), OrderKeywordDto.ASC)
            .build();
    PivotTable pt = queryExecutor.executePivotQuery(new PivotTableQueryDto(
            query,
            List.of(new TableField("product"), new TableField("region")),
            List.of(new TableField("month"))));
    pt.show();
    jsonpt(pt);
  }
}
