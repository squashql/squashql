package io.squashql.query;

import io.squashql.query.builder.Query;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.compiled.CompiledOrderBy;
import io.squashql.query.database.*;
import io.squashql.query.dto.*;
import io.squashql.store.Store;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;
import lombok.AllArgsConstructor;
import org.junit.jupiter.api.Test;

import java.util.*;

import static io.squashql.query.Functions.*;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.query.dto.JoinType.INNER;
import static io.squashql.query.dto.JoinType.LEFT;
import static io.squashql.transaction.DataLoader.SCENARIO_FIELD_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSQLTranslator {

  private static final String BASE_STORE_NAME = "baseStore";

  private static class TestResolver extends QueryResolver {
    private static final Map<String, Store> stores = new HashMap<>();

    static {
      stores.put(BASE_STORE_NAME, new Store(BASE_STORE_NAME, List.of(
              new TableTypedField(BASE_STORE_NAME, "id", String.class),
              new TableTypedField(BASE_STORE_NAME, "type", String.class),
              new TableTypedField(BASE_STORE_NAME, "delta", double.class),
              new TableTypedField(BASE_STORE_NAME, "pnl", double.class),
              new TableTypedField(BASE_STORE_NAME, "country", String.class),
              new TableTypedField(BASE_STORE_NAME, "a", String.class),
              new TableTypedField(BASE_STORE_NAME, "b", String.class),
              new TableTypedField(BASE_STORE_NAME, "c", String.class),
              new TableTypedField(BASE_STORE_NAME, "c1", String.class),
              new TableTypedField(BASE_STORE_NAME, "c2", String.class),
              new TableTypedField(BASE_STORE_NAME, "c3", String.class),
              new TableTypedField(BASE_STORE_NAME, "scenario", String.class))));

      String recommendation = "recommendation";
      stores.put(recommendation, new Store(recommendation, List.of(
              new TableTypedField(recommendation, "finalprice", double.class),
              new TableTypedField(recommendation, "recoprice", double.class),
              new TableTypedField(recommendation, "initial_price", double.class))));
      String product = "product";
      stores.put(product, new Store(product, List.of(
              new TableTypedField(product, "tva_rate", double.class))));

      stores.put("table1", new Store("table1", List.of(
              new TableTypedField("table1", "table1_id", String.class),
              new TableTypedField("table1", "table1_field_2", String.class),
              new TableTypedField("table1", "table1_field_3", String.class))));
      stores.put("table2", new Store("table2", List.of(
              new TableTypedField("table2", "table2_id", String.class),
              new TableTypedField("table2", "table2_field_1", String.class))));
      stores.put("table3", new Store("table3", List.of(
              new TableTypedField("table3", "table3_id", String.class))));
      stores.put("table4", new Store("table4", List.of(
              new TableTypedField("table4", "table4_id_1", String.class),
              new TableTypedField("table4", "table4_id_2", String.class))));
    }

    private final int limit;

    public TestResolver(QueryDto query) {
      super(query, stores);
      this.limit = query.limit;
    }

    DatabaseQuery toDatabaseQuery() {
      // Keep the order
      List<CompiledMeasure> measures = getQuery().measures.stream().map(measure -> getMeasures().get(measure)).filter(Objects::nonNull).toList();
      return new DatabaseQuery(getScope().copyWithNewLimit(this.limit), measures);
    }

    @Override
    protected void checkQuery(QueryDto query) {
      // nothing to do
    }
  }

  @AllArgsConstructor
  static class SQLTranslatorQueryRewriter implements QueryRewriter {

    @Override
    public String escapeAlias(String alias) {
      return SqlUtils.backtickEscape(alias);
    }

    @Override
    public String fieldName(String field) {
      return SqlUtils.backtickEscape(field);
    }

    @Override
    public String tableName(String table) {
      // Should be different then cteName() to make sure tableName and cteName are correctly differentiated and used properly
      return SqlUtils.backtickEscape("dataset." + table);
    }

    @Override
    public String cteName(String cteName) {
      return SqlUtils.backtickEscape(cteName);
    }

    @Override
    public boolean usePartialRollupSyntax() {
      return true;
    }
  }

  public static String translate(DatabaseQuery query) {
    return SQLTranslator.translate(query, new SQLTranslatorQueryRewriter());
  }

  private DatabaseQuery compileQuery(QueryDto query) {
    return new TestResolver(query).toDatabaseQuery();
  }

  private CompiledMeasure compiledMeasure(Measure m) {
    return new TestResolver(new QueryDto().table("fake")).compileMeasure(m, true);
  }

  private TypedField compileField(Field field) {
    return new TestResolver(new QueryDto().table("fake")).resolveField(field);
  }

  private DatabaseQuery compileQuery(QueryDto query, Map<String, Store> stores) {
    return new QueryResolver(query, stores) {
      DatabaseQuery toDatabaseQuery() {
        return new DatabaseQuery(getScope().copyWithNewLimit(query.limit), new ArrayList<>(getMeasures().values()));
      }
    }.toDatabaseQuery();
  }

  @Test
  void testGrandTotal() {
    final QueryDto query = new QueryDto()
            .withMeasure(new AggregatedMeasure("pnl.sum", "pnl", "sum"))
            .withMeasure(new AggregatedMeasure("delta.sum", "delta", "sum"))
            .withMeasure(new AggregatedMeasure("pnl.avg", "pnl", "avg"))
            .withMeasure(new AggregatedMeasure("mean pnl", "pnl", "avg"))
            .table(BASE_STORE_NAME);
    assertThat(translate(compileQuery(query)))
            .isEqualTo("select sum(`pnl`) as `pnl.sum`, sum(`delta`) as `delta.sum`, avg(`pnl`) as `pnl.avg`, avg(`pnl`) as `mean pnl` from `dataset.baseStore`");
  }

  @Test
  void testLimit() {
    final QueryDto query = new QueryDto()
            .withMeasure(new AggregatedMeasure("pnl.sum", "pnl", "sum"))
            .withLimit(8)
            .table(BASE_STORE_NAME);

    assertThat(translate(compileQuery(query)))
            .isEqualTo("select sum(`pnl`) as `pnl.sum` from `dataset.baseStore` limit 8");
  }

  @Test
  void testGroupBy() {
    final QueryDto query = new QueryDto()
            .withColumn(tableField(SCENARIO_FIELD_NAME))
            .withColumn(tableField("type"))
            .withMeasure(new AggregatedMeasure("pnl.sum", "pnl", "sum"))
            .withMeasure(new AggregatedMeasure("delta.sum", "delta", "sum"))
            .withMeasure(new AggregatedMeasure("pnl.avg", "pnl", "avg"))
            .table(BASE_STORE_NAME);

    assertThat(translate(compileQuery(query)))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`) as `pnl.sum`, sum(`delta`) as `delta.sum`, avg(`pnl`) as `pnl.avg` from `dataset.baseStore` group by " +
                    "`scenario`, `type`");
  }

  @Test
  void testGroupByWithFullName() {
    final QueryDto query = new QueryDto()
            .withColumn(tableField(SqlUtils.getFieldFullName(BASE_STORE_NAME, SCENARIO_FIELD_NAME)))
            .withColumn(tableField(SqlUtils.getFieldFullName(BASE_STORE_NAME, "type")))
            .withMeasure(new AggregatedMeasure("pnl.sum", SqlUtils.getFieldFullName(BASE_STORE_NAME, "pnl"), "sum"))
            .withMeasure(new AggregatedMeasure("delta.sum", SqlUtils.getFieldFullName(BASE_STORE_NAME, "delta"), "sum"))
            .table(BASE_STORE_NAME);

    assertThat(translate(compileQuery(query)))
            .isEqualTo("select `dataset.baseStore`.`scenario`, `dataset.baseStore`.`type`, sum(`dataset.baseStore`.`pnl`) as `pnl.sum`, sum(`dataset.baseStore`.`delta`) as `delta.sum` from `dataset.baseStore` group by " +
                    "`dataset.baseStore`.`scenario`, `dataset.baseStore`.`type`");
  }

  @Test
  void testDifferentMeasures() {
    final QueryDto query = new QueryDto()
            .table(BASE_STORE_NAME)
            .withMeasure(new AggregatedMeasure("pnl.sum", "pnl", "sum"))
            .withMeasure(new ExpressionMeasure("indice", "100 * sum(`delta`) / sum(`pnl`)"));

    assertThat(translate(compileQuery(query)))
            .isEqualTo("select sum(`pnl`) as `pnl.sum`, 100 * sum(`delta`) / sum(`pnl`) as `indice` from `dataset.baseStore`");
  }

  @Test
  void testWithFullRollup() {
    final Field scenario = tableField(SCENARIO_FIELD_NAME);
    final Field type = tableField("type");
    final QueryDto query = new QueryDto()
            .withColumn(scenario)
            .withColumn(type)
            .withRollup(scenario)
            .withRollup(type)
            .withMeasure(new AggregatedMeasure("pnl.sum", "price", "sum"))
            .table(BASE_STORE_NAME);

    assertThat(translate(compileQuery(query)))
            .isEqualTo("""
                    select `scenario`, `type`,
                     sum(`price`) as `pnl.sum`
                     from `dataset.baseStore` group by rollup(`scenario`, `type`)
                    """.replaceAll(System.lineSeparator(), ""));
  }

  @Test
  void testWithFullRollupWithFullName() {
    final Field scenario = tableField(SqlUtils.getFieldFullName(BASE_STORE_NAME, SCENARIO_FIELD_NAME));
    final Field type = tableField(SqlUtils.getFieldFullName(BASE_STORE_NAME, "type"));
    final QueryDto query = new QueryDto()
            .withColumn(scenario)
            .withColumn(type)
            .withRollup(scenario)
            .withRollup(type)
            .withMeasure(new AggregatedMeasure("pnl.sum", "price", "sum"))
            .table(BASE_STORE_NAME);

    assertThat(translate(compileQuery(query)))
            .isEqualTo("""
                    select `dataset.baseStore`.`scenario`, `dataset.baseStore`.`type`,
                     sum(`price`) as `pnl.sum`
                     from `dataset.baseStore` group by rollup(`dataset.baseStore`.`scenario`, `dataset.baseStore`.`type`)
                    """.replaceAll(System.lineSeparator(), ""));
  }

  @Test
  void testWithPartialRollup() {
    final QueryDto query = new QueryDto()
            .withColumn(tableField(SCENARIO_FIELD_NAME))
            .withColumn(tableField("type"))
            .withRollup(tableField(SCENARIO_FIELD_NAME))
            .withMeasure(new AggregatedMeasure("pnl.sum", "price", "sum"))
            .table(BASE_STORE_NAME);

    assertThat(translate(compileQuery(query)))
            .isEqualTo("select `scenario`, `type`," +
                    " sum(`price`) as `pnl.sum`" +
                    " from `dataset.baseStore` group by `type`, rollup(`scenario`)");
  }

  @Test
  void testJoins() {
    TableDto baseStore = new TableDto(BASE_STORE_NAME);
    TableDto table1 = new TableDto("table1");
    TableDto table2 = new TableDto("table2");
    TableDto table3 = new TableDto("table3");
    TableDto table4 = new TableDto("table4");

    baseStore.joins.add(new JoinDto(table1, INNER, criterion(baseStore.name + ".id", table1.name + ".table1_id", ConditionType.EQ)));
    baseStore.joins.add(new JoinDto(table2, LEFT, criterion(baseStore.name + ".id", table2.name + ".table2_id", ConditionType.EQ)));

    table1.joins.add(new JoinDto(table4, INNER, all(
            criterion(table1.name + ".table1_field_2", table4.name + ".table4_id_1", ConditionType.EQ),
            criterion(table1.name + ".table1_field_3", table4.name + ".table4_id_2", ConditionType.EQ)
    )));
    table2.joins.add(new JoinDto(table3, INNER, criterion(table2.name + ".table2_field_1", table3.name + ".table3_id", ConditionType.EQ)));

    final QueryDto query = new QueryDto()
            .table(baseStore)
            .withMeasure(new AggregatedMeasure("pnl.avg", "pnl", "avg"));

    assertThat(translate(compileQuery(query)))
            .isEqualTo("select avg(`pnl`) as `pnl.avg` from `dataset.baseStore`"
                    + " inner join `dataset.table1` on `dataset.baseStore`.`id` = `dataset.table1`.`table1_id`"
                    + " inner join `dataset.table4` on (`dataset.table1`.`table1_field_2` = `dataset.table4`.`table4_id_1` and `dataset.table1`.`table1_field_3` = `dataset.table4`.`table4_id_2`)"
                    + " left join `dataset.table2` on `dataset.baseStore`.`id` = `dataset.table2`.`table2_id`"
                    + " inner join `dataset.table3` on `dataset.table2`.`table2_field_1` = `dataset.table3`.`table3_id`"
            );
  }

  @Test
  void testJoinsEquijoinsMultipleCondCrossTables() {
    TableDto a = new TableDto("A");
    TableDto b = new TableDto("B");
    TableDto c = new TableDto("C");
    // should be able to ref. both column of A and B in the join.

    a.join(b, INNER, criterion(a.name + ".a_id", b.name + ".b_id", ConditionType.EQ));
    a.join(c, LEFT, all(
            criterion(c.name + ".c_other_id", b.name + ".b_other_id", ConditionType.EQ),
            criterion(c.name + ".c_f", a.name + ".a_f", ConditionType.EQ)
    ));

    final Map<String, Store> stores = Map.of(
            "A", new Store("A", List.of(new TableTypedField("A", "a_id", int.class), new TableTypedField("A", "a_f", int.class), new TableTypedField("A", "y", int.class))),
            "B", new Store("B", List.of(new TableTypedField("B", "b_id", int.class), new TableTypedField("B", "b_other_id", int.class))),
            "C", new Store("C", List.of(new TableTypedField("c", "a_id", int.class), new TableTypedField("C", "c_f", int.class), new TableTypedField("C", "c_other_id", int.class)))
    );

    final QueryDto query = new QueryDto().table(a).withColumn(tableField("A.y"));

    assertThat(translate(compileQuery(query, stores)))
            .isEqualTo("select `dataset.A`.`y` from `dataset.A` " +
                    "inner join `dataset.B` on `dataset.A`.`a_id` = `dataset.B`.`b_id` " +
                    "left join `dataset.C` on (`dataset.C`.`c_other_id` = `dataset.B`.`b_other_id` and `dataset.C`.`c_f` = `dataset.A`.`a_f`) " +
                    "group by `dataset.A`.`y`");
  }

  @Test
  void testJoinsNestedQuery() {
    QueryDto subQuery = Query.from(BASE_STORE_NAME)
            .select(tableFields(List.of("a", "b")), List.of(sum("pnl_sum", "pnl")))
            .build();

    VirtualTableDto virtualTable = new VirtualTableDto("virtual", List.of("id", "value"), List.of(List.of(0, "0"), List.of(1, "1")));

    QueryDto query = Query.from(subQuery)
            .join(virtualTable, INNER)
            .on(criterion(BASE_STORE_NAME + ".a", virtualTable.name + ".id", ConditionType.EQ))
            .select(tableFields(List.of("b", "value")), List.of(avg("pnl_avg", "pnl_sum")))
            .build();

    assertThat(translate(compileQuery(query))).isEqualTo("" +
            "with `virtual` as (select 0 as `id`, '0' as `value` union all select 1 as `id`, '1' as `value`) " +
            "select `b`, `value`, avg(`pnl_sum`) as `pnl_avg` from (" +
            "select `a`, `b`, sum(`pnl`) as `pnl_sum` from `dataset.baseStore` group by `a`, `b`" +
            ") " +
            "inner join `virtual` on `dataset.baseStore`.`a` = `virtual`.`id` group by `b`, `value`");
  }

  @Test
  void testConditionsWithValue() {
    final QueryDto query = new QueryDto()
            .withColumn(tableField(SCENARIO_FIELD_NAME))
            .withColumn(tableField("type"))
            .withMeasure(new AggregatedMeasure("pnl.sum", "pnl", "sum"))
            .withWhereCriteria(all(
                    criterion(SCENARIO_FIELD_NAME, or(eq("base"), eq("s1"), eq("s2"))),
                    criterion("delta", ge(123d)),
                    criterion("type", or(eq("A'"), eq("B"))),
                    criterion("pnl", lt(10d)),
                    criterion(minus(new TableField("pnl"), new ConstantField(1)), lt(11d)))
            )
            .table(BASE_STORE_NAME);
    assertThat(translate(compileQuery(query)))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`) as `pnl.sum` from `dataset.baseStore`"
                    + " where (((`scenario` = 'base' or `scenario` = 's1') or `scenario` = 's2')"
                    + " and `delta` >= 123.0 and (`type` = 'A''' or `type` = 'B') and `pnl` < 10.0 and (`pnl`-1) < 11.0)"
                    + " group by `scenario`, `type`"
            );
  }

  @Test
  void testConditionWithValueFullPath() {
    TableField field = new TableField(BASE_STORE_NAME, SCENARIO_FIELD_NAME);
    TableTypedField typedField = new TableTypedField(BASE_STORE_NAME, SCENARIO_FIELD_NAME, String.class);
    final QueryDto query = new QueryDto()
            .withColumn(field)
            .withMeasure(new AggregatedMeasure("pnl.sum", "pnl", "sum"))
            .withWhereCriteria(criterion(SqlUtils.getFieldFullName(typedField), or(
                    eq("base"),
                    eq("s2"),
                    isNull())))
            .table(BASE_STORE_NAME);
    assertThat(translate(compileQuery(query)))
            .isEqualTo("select `dataset.baseStore`.`scenario`, sum(`pnl`) as `pnl.sum` from `dataset.baseStore`"
                    + " where ((`dataset.baseStore`.`scenario` = 'base' or `dataset.baseStore`.`scenario` = 's2') or `dataset.baseStore`.`scenario` is null)"
                    + " group by `dataset.baseStore`.`scenario`"
            );
  }

  @Test
  void testSelectFromSelectWithAlias() {
    // Kind of leaf agg. !!!
    TableDto a = new TableDto("a");
    QueryDto subQuery = new QueryDto()
            .table(a)
            .withColumn(tableField("c1").as("alias_c1"))
            .withColumn(tableField("c3"))
            .withMeasure(avg("mean", "c2"));

    QueryDto query = new QueryDto()
            .table(subQuery)
            .withColumn(tableField("c3")) // c3 needs to be in the subquery
            .withMeasure(sum("sum c1", new AliasedField("alias_c1"))) // alias_c1 is a field from the subquery
            .withMeasure(sum("sum GT", "mean"))
            .withWhereCriteria(criterion("type", eq("myType")));
    assertThat(translate(compileQuery(query)))
            .isEqualTo("select `c3`, sum(`alias_c1`) as `sum c1`, sum(`mean`) as `sum GT` from (select `c1` as `alias_c1`, `c3`, avg(`c2`) as `mean` from `dataset.a` group by `alias_c1`, `c3`) where `type` = 'myType' group by `c3`");
  }

  @Test
  void testBinaryOperationMeasure() {
    TableDto a = new TableDto("a");
    Measure plus = plus("plus", sum("pnl.sum", "pnl"), avg("delta.avg", "delta"));
    Measure divide = divide("divide", plus, Functions.decimal(100));
    final QueryDto query = new QueryDto()
            .table(a)
            .withMeasure(plus)
            .withMeasure(divide);
    assertThat(translate(compileQuery(query)))
            .isEqualTo("select (sum(`pnl`)+avg(`delta`)) as `plus`, ((sum(`pnl`)+avg(`delta`))/100.0) as `divide` from `dataset.a`");
  }

  @Test
  void testAggregatedMeasures() {
    TableDto a = new TableDto(BASE_STORE_NAME);
    QueryDto query = new QueryDto()
            .table(a)
            .withMeasure(sum("pnlSum", "pnl"))
            .withMeasure(sumIf("pnlSumFiltered", "pnl", criterion("country", eq("france"))));
    assertThat(translate(compileQuery(query)))
            .isEqualTo("select sum(`pnl`) as `pnlSum`, sum(case when `country` = 'france' then `pnl` end) as `pnlSumFiltered` from `dataset.baseStore`");

    // With full path
    query = new QueryDto()
            .table(a)
            .withMeasure(sum("pnlSum", a.name + ".pnl"))
            .withMeasure(sumIf("pnlSumFiltered", a.name + ".pnl", criterion(a.name + ".country", eq("france"))));
    String format = "select sum(`%1$s`.`pnl`) as `pnlSum`, sum(case when `%1$s`.`country` = 'france' then `%1$s`.`pnl` end) as `pnlSumFiltered` from `%1$s`";
    assertThat(translate(compileQuery(query))).isEqualTo(String.format(format, "dataset.baseStore"));
  }

  @Test
  void testVirtualTable() {
    TableDto main = new TableDto(BASE_STORE_NAME);
    VirtualTableDto virtualTable = new VirtualTableDto(
            "virtual",
            List.of("a", "b"),
            List.of(List.of(0, "0"), List.of(1, "1")));
    main.join(new TableDto(virtualTable.name), INNER, criterion("id", "a", ConditionType.EQ));
    QueryDto query = new QueryDto()
            .table(main)
            .withMeasure(sum("pnl.sum", "pnl"))
            .withColumn(tableField("id"))
            .withColumn(tableField("b"));
    query.virtualTableDtos = List.of(virtualTable);
    String expected = String.format("with `%2$s` as (select 0 as `a`, '0' as `b` union all select 1 as `a`, '1' as `b`) " +
                    "select `id`, `b`, sum(`pnl`) as `pnl.sum` from `%1$s` " +
                    "inner join `%2$s` on `id` = `a` group by `id`, `b`",
            "dataset.baseStore", virtualTable.name);
    assertThat(translate(compileQuery(query))).isEqualTo(expected);
  }

  @Test
  void testVirtualTables() {
    TableDto main = new TableDto(BASE_STORE_NAME);
    VirtualTableDto virtualTable1 = new VirtualTableDto(
            "virtual1",
            List.of("a1", "b1"),
            List.of(List.of(0, "0"), List.of(1, "1")));
    VirtualTableDto virtualTable2 = new VirtualTableDto(
            "virtual2",
            List.of("a2", "b2"),
            List.of(List.of(0, "0"), List.of(1, "1")));
    main.join(new TableDto(virtualTable1.name), INNER, criterion(BASE_STORE_NAME + ".id", virtualTable1.name + ".a1", ConditionType.EQ));
    main.join(new TableDto(virtualTable2.name), INNER, criterion(BASE_STORE_NAME + ".id", virtualTable2.name + ".a2", ConditionType.EQ));
    QueryDto query = new QueryDto()
            .table(main)
            .withMeasure(sum("pnl.sum", "pnl"))
            .withColumn(tableField("id"))
            .withColumn(tableField(virtualTable1.name + ".b1"))
            .withColumn(tableField(virtualTable2.name + ".b2"));
    query.virtualTableDtos = List.of(virtualTable1, virtualTable2);
    String expected = "with " +
            "`virtual1` as (select 0 as `a1`, '0' as `b1` union all select 1 as `a1`, '1' as `b1`), " +
            "`virtual2` as (select 0 as `a2`, '0' as `b2` union all select 1 as `a2`, '1' as `b2`) " +
            "select `id`, `virtual1`.`b1`, `virtual2`.`b2`, sum(`pnl`) as `pnl.sum` " +
            "from `dataset.baseStore` " +
            "inner join `virtual1` on `dataset.baseStore`.`id` = `virtual1`.`a1` " +
            "inner join `virtual2` on `dataset.baseStore`.`id` = `virtual2`.`a2` " +
            "group by `id`, `virtual1`.`b1`, `virtual2`.`b2`";
    assertThat(translate(compileQuery(query))).isEqualTo(expected);
  }

  @Test
  void testVirtualTableFullName() {
    final TableDto main = new TableDto(BASE_STORE_NAME);
    final VirtualTableDto virtualTable = new VirtualTableDto(
            "virtual",
            List.of("a", "b"),
            List.of(List.of(0, "0"), List.of(1, "1")));
    main.join(new TableDto(virtualTable.name), INNER, criterion(BASE_STORE_NAME + ".id", virtualTable.name + ".a", ConditionType.EQ));
    final QueryDto query = new QueryDto()
            .table(main)
            .withMeasure(sum("pnl.sum", "pnl"))
            .withColumn(tableField("id"))
            .withColumn(tableField("b"));
    query.virtualTableDtos = List.of(virtualTable);

    String expected = String.format("with `%2$s` as (select 0 as `a`, '0' as `b` union all select 1 as `a`, '1' as `b`) " +
                    "select `id`, `b`, sum(`pnl`) as `pnl.sum` from `%1$s` " +
                    "inner join `%2$s` on `%1$s`.`id` = `%2$s`.`a` group by `id`, `b`",
            "dataset.baseStore", virtualTable.name);
    assertThat(translate(compileQuery(query))).isEqualTo(expected);
  }

  @Test
  void testGroupingSets() {
    Field a = tableField("a");
    Field b = tableField("b");
    QueryDto query = new QueryDto()
            .withColumn(a)
            .withColumn(b)
            .withMeasure(new AggregatedMeasure("pnl.sum", "pnl", "sum"))
            .table(BASE_STORE_NAME);
    query.groupingSets = List.of(
            List.of(), // GT
            List.of(a), // total a
            List.of(a, b));
    assertThat(translate(compileQuery(query)))
            .satisfiesAnyOf(
                    sql -> assertThat(sql).isEqualTo("select `a`, `b`, sum(`pnl`) as `pnl.sum` from `dataset.baseStore` group by grouping sets((), (`a`), (`b`,`a`))"),
                    sql -> assertThat(sql).isEqualTo("select `a`, `b`, sum(`pnl`) as `pnl.sum` from `dataset.baseStore` group by grouping sets((), (`a`), (`a`,`b`))")
            );
  }

  @Test
  void testComplexAggregatedMeasures() {
    Field finalPrice = new TableField("recommendation.finalprice");
    Field recoPrice = new TableField("recommendation.recoprice");
    Measure measure = sumIf("increase_sum", minus(finalPrice, recoPrice), all(
            criterion(finalPrice, recoPrice, ConditionType.GT),
            criterion(recoPrice, gt(0))
    ));

    DefaultQueryRewriter qr = new DefaultQueryRewriter(null);
    String expression = compiledMeasure(measure).sqlExpression(qr, true);
    assertThat(expression)
            .isEqualTo("sum(case when (`recommendation`.`finalprice` > `recommendation`.`recoprice` and `recommendation`.`recoprice` > 0)" +
                    " then (`recommendation`.`finalprice`-`recommendation`.`recoprice`) end)" +
                    " as `increase_sum`");

    Field initial_price = new TableField("recommendation.initial_price");
    Field one = new ConstantField(1);
    Field tvaRate = new TableField("product.tva_rate");
    Measure whatever = sum("whatever", divide(initial_price, plus(one, tvaRate)));
    assertThat(compiledMeasure(whatever).sqlExpression(qr, true))
            .isEqualTo("sum((`recommendation`.`initial_price`/(1+`product`.`tva_rate`))) as `whatever`");
  }

  @Test
  void testComplexFieldCalculation() {
    DefaultQueryRewriter qr = new DefaultQueryRewriter(null);
    Field a = new TableField("a");
    Field b = new TableField("b");
    Field c = new TableField("c");

    BinaryOperationField f1_minus_f2 = new BinaryOperationField(BinaryOperator.MINUS, a, b);
    BinaryOperationField divide = new BinaryOperationField(BinaryOperator.DIVIDE, f1_minus_f2, a);
    BinaryOperationField multiply = new BinaryOperationField(BinaryOperator.MULTIPLY, divide, c);
    BinaryOperationField plus = new BinaryOperationField(BinaryOperator.PLUS, multiply, new ConstantField(2));
    assertThat(compileField(plus).sqlExpression(qr))
            .isEqualTo("((((`a`-`b`)/`a`)*`c`)+2)");
  }

  @Test
  void testOrderBy() {
    SQLTranslatorQueryRewriter qr = new SQLTranslatorQueryRewriter();
    TableTypedField stringField = new TableTypedField("store", "field", String.class);
    TableTypedField intField = new TableTypedField("store", "field", int.class);
    String sql = new CompiledOrderBy(stringField, new SimpleOrderDto(OrderKeywordDto.ASC)).sqlExpression(qr);
    assertThat(sql).isEqualTo("`dataset.store`.`field` asc nulls first");
    sql = new CompiledOrderBy(stringField, new SimpleOrderDto(OrderKeywordDto.DESC)).sqlExpression(qr);
    assertThat(sql).isEqualTo("`dataset.store`.`field` desc nulls first");
    sql = new CompiledOrderBy(intField, new ExplicitOrderDto(List.of(2023, 2027))).sqlExpression(qr);
    assertThat(sql).isEqualTo("case when `dataset.store`.`field` is null then 1 when `dataset.store`.`field` = 2023 then 2 when `dataset.store`.`field` = 2027 then 3 else 4 end");
    sql = new CompiledOrderBy(stringField, new ExplicitOrderDto(List.of(2023, 2027))).sqlExpression(qr);
    // When field is a string, values should be escaped
    assertThat(sql).isEqualTo("case when `dataset.store`.`field` is null then 1 when `dataset.store`.`field` = '2023' then 2 when `dataset.store`.`field` = '2027' then 3 else 4 end");
  }

  @Test
  void testCriteria() {
    Field a = tableField("a");
    Field b = tableField("b");
    // The following conditions are equivalent
    CriteriaDto c1 = criterion(a, b, ConditionType.EQ);
    CriteriaDto c2 = criterion(a, eq(b));
    QueryDto q1 = Query.from(BASE_STORE_NAME)
            .where(c1)
            .select(List.of(a), List.of())
            .build();
    QueryDto q2 = Query.from(BASE_STORE_NAME)
            .where(c2)
            .select(List.of(a), List.of())
            .build();
    assertThat(translate(compileQuery(q1)))
            .isEqualTo(translate(compileQuery(q2)))
            .isEqualTo("select `a` from `dataset.baseStore` where `a` = `b` group by `a`");
  }

  @Test
  void testCriteriaWithConstant() {
    Field a = tableField("a"); // it is a String
    {
      // The following conditions are equivalent
      ConstantField cf = new ConstantField("hello");
      CriteriaDto c1 = criterion(a, cf, ConditionType.EQ);
      CriteriaDto c1bis = criterion(cf, a, ConditionType.EQ);
      CriteriaDto c2 = criterion(a, eq(cf));
      QueryDto q1 = Query.from(BASE_STORE_NAME)
              .where(c1)
              .select(List.of(a), List.of())
              .build();
      QueryDto q1bis = Query.from(BASE_STORE_NAME)
              .where(c1bis)
              .select(List.of(a), List.of())
              .build();
      QueryDto q2 = Query.from(BASE_STORE_NAME)
              .where(c2)
              .select(List.of(a), List.of())
              .build();
      assertThat(translate(compileQuery(q1)))
              .isEqualTo(translate(compileQuery(q1bis)))
              .isEqualTo(translate(compileQuery(q2)))
              .isEqualTo("select `a` from `dataset.baseStore` where `a` = 'hello' group by `a`");
    }

    {
      Field pnl = tableField("pnl"); // it is a double
      // The following conditions are equivalent
      ConstantField cf = new ConstantField(5);
      CriteriaDto c1 = criterion(pnl, cf, ConditionType.GT);
      CriteriaDto c1bis = criterion(cf, pnl, ConditionType.GT);
      CriteriaDto c2 = criterion(pnl, gt(cf));
      QueryDto q1 = Query.from(BASE_STORE_NAME)
              .where(c1)
              .select(List.of(pnl), List.of())
              .build();
      QueryDto q1bis = Query.from(BASE_STORE_NAME)
              .where(c1bis)
              .select(List.of(pnl), List.of())
              .build();
      QueryDto q2 = Query.from(BASE_STORE_NAME)
              .where(c2)
              .select(List.of(pnl), List.of())
              .build();
      assertThat(translate(compileQuery(q1)))
              .isEqualTo(translate(compileQuery(q1bis)))
              .isEqualTo(translate(compileQuery(q2)))
              .isEqualTo("select `pnl` from `dataset.baseStore` where `pnl` > 5 group by `pnl`");
    }
  }
}
