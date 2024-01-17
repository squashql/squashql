package io.squashql.query;

import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.DefaultQueryRewriter;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.*;
import io.squashql.store.Store;
import io.squashql.type.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.squashql.query.Functions.*;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.database.SQLTranslator.translate;
import static io.squashql.query.dto.JoinType.INNER;
import static io.squashql.query.dto.JoinType.LEFT;
import static io.squashql.transaction.DataLoader.SCENARIO_FIELD_NAME;

public class TestSQLTranslator {

  private static final String BASE_STORE_NAME = "baseStore";
  private static final String BASE_STORE_NAME_ESCAPED = SqlUtils.backtickEscape(BASE_STORE_NAME);

  private static class TestResolver extends QueryResolver {
    private final int limit;

    public TestResolver(QueryDto query) {
      super(query, new HashMap<>());
      this.limit = query.limit;
    }

    @Override
    protected TypedField resolveField(Field field) {
      Function<String, Class<?>> type = f -> switch (f) {
        case "pnl", BASE_STORE_NAME + "." + "pnl" -> double.class;
        case "delta", BASE_STORE_NAME + "." + "delta" -> Double.class;
        default -> String.class;
      };
      if (field instanceof BinaryOperationField bf) {
        return new BinaryOperationTypedField(bf.operator, resolveField(bf.leftOperand), resolveField(bf.rightOperand), field.alias());
      } else if (field instanceof ConstantField cf) {
        return new ConstantTypedField(cf.value);
      } else if (field instanceof AliasedField) {
        return new AliasedTypedField(field.alias());
      }
      String[] split = field.name().split("\\.");
      if (split.length > 1) {
        String tableName = split[0];
        String fieldNameInTable = split[1];
        return new TableTypedField(tableName, fieldNameInTable, type.apply(fieldNameInTable), field.alias());
      } else {
        return new TableTypedField(null, split[0], type.apply(split[0]), field.alias());
      }
    }

    DatabaseQuery toDatabaseQuery() {
      final DatabaseQuery compiled = toDatabaseQuery(getScope(), this.limit);
      // Keep the order
      for (Measure measure : getQuery().measures) {
        CompiledMeasure compiledMeasure = getMeasures().get(measure);
        if (compiledMeasure != null) {
          compiled.withMeasure(compiledMeasure);
        }
      }
      return compiled;
    }

    @Override
    protected void checkQuery(QueryDto query) {
      // nothing to do
    }
  }

  private DatabaseQuery compileQuery(QueryDto query) {
    return new TestResolver(query).toDatabaseQuery();
  }

  private CompiledMeasure compiledMeasure(Measure m) {
    return new TestResolver(new QueryDto()).compileMeasure(m, true);
  }

  private TypedField compileField(Field field) {
    return new TestResolver(new QueryDto()).resolveField(field);
  }

  private DatabaseQuery compileQuery(QueryDto query, Map<String, Store> stores) {
    return new QueryResolver(query, stores) {
      DatabaseQuery toDatabaseQuery() {
        final DatabaseQuery compiled = toDatabaseQuery(getScope(), query.limit);
        getMeasures().values().forEach(compiled::withMeasure);
        return compiled;
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
    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("select sum(`pnl`) as `pnl.sum`, sum(`delta`) as `delta.sum`, avg(`pnl`) as `pnl.avg`, avg(`pnl`) as `mean pnl` from " + BASE_STORE_NAME_ESCAPED);
  }

  @Test
  void testLimit() {
    final QueryDto query = new QueryDto()
            .withMeasure(new AggregatedMeasure("pnl.sum", "pnl", "sum"))
            .withLimit(8)
            .table(BASE_STORE_NAME);

    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("select sum(`pnl`) as `pnl.sum` from `" + BASE_STORE_NAME + "` limit 8");
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

    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`) as `pnl.sum`, sum(`delta`) as `delta.sum`, avg(`pnl`) as `pnl.avg` from " + BASE_STORE_NAME_ESCAPED + " group by " +
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

    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("select `baseStore`.`scenario`, `baseStore`.`type`, sum(`baseStore`.`pnl`) as `pnl.sum`, sum(`baseStore`.`delta`) as `delta.sum` from " + BASE_STORE_NAME_ESCAPED + " group by " +
                    "`baseStore`.`scenario`, `baseStore`.`type`");
  }

  @Test
  void testDifferentMeasures() {
    final QueryDto query = new QueryDto()
            .table(BASE_STORE_NAME)
            .withMeasure(new AggregatedMeasure("pnl.sum", "pnl", "sum"))
            .withMeasure(new ExpressionMeasure("indice", "100 * sum(`delta`) / sum(`pnl`)"));

    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("select sum(`pnl`) as `pnl.sum`, 100 * sum(`delta`) / sum(`pnl`) as `indice` from " + BASE_STORE_NAME_ESCAPED);
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

    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("""
                    select `scenario`, `type`,
                     sum(`price`) as `pnl.sum`
                     from `baseStore` group by rollup(`scenario`, `type`)
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

    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("""
                    select `baseStore`.`scenario`, `baseStore`.`type`,
                     sum(`price`) as `pnl.sum`
                     from `baseStore` group by rollup(`baseStore`.`scenario`, `baseStore`.`type`)
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

    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("select `scenario`, `type`," +
                    " sum(`price`) as `pnl.sum`" +
                    " from `baseStore` group by `type`, rollup(`scenario`)");
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

    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("select avg(`pnl`) as `pnl.avg` from " + BASE_STORE_NAME_ESCAPED
                    + " inner join `table1` on " + BASE_STORE_NAME_ESCAPED + ".`id` = `table1`.`table1_id`"
                    + " inner join `table4` on (`table1`.`table1_field_2` = `table4`.`table4_id_1` and `table1`.`table1_field_3` = `table4`.`table4_id_2`)"
                    + " left join `table2` on " + BASE_STORE_NAME_ESCAPED + ".`id` = `table2`.`table2_id`"
                    + " inner join `table3` on `table2`.`table2_field_1` = `table3`.`table3_id`"
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

    Assertions.assertThat(translate(compileQuery(query, stores)))
            .isEqualTo("select `A`.`y` from `A` " +
                    "inner join `B` on `A`.`a_id` = `B`.`b_id` " +
                    "left join `C` on (`C`.`c_other_id` = `B`.`b_other_id` and `C`.`c_f` = `A`.`a_f`) " +
                    "group by `A`.`y`");
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
    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`) as `pnl.sum` from " + BASE_STORE_NAME_ESCAPED
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
    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("select `baseStore`.`scenario`, sum(`pnl`) as `pnl.sum` from " + BASE_STORE_NAME_ESCAPED
                    + " where ((`baseStore`.`scenario` = 'base' or `baseStore`.`scenario` = 's2') or `baseStore`.`scenario` is null)"
                    + " group by `baseStore`.`scenario`"
            );
  }

  @Test
  void testSelectFromSelectWithAlias() {
    // Kind of leaf agg. !!!
    TableDto a = new TableDto("a");
    final QueryDto subQuery = new QueryDto()
            .table(a)
            .withColumn(tableField("c1").as("alias_c1"))
            .withColumn(tableField("c3"))
            .withMeasure(avg("mean", "c2"));

    final QueryDto query = new QueryDto()
            .table(subQuery)
            .withColumn(tableField("c3")) // c3 needs to be in the subquery
            .withMeasure(sum("sum c1", new AliasedField("alias_c1"))) // alias_c1 is a field from the subquery
            .withMeasure(sum("sum GT", "mean"))
            .withWhereCriteria(criterion("type", eq("myType")));
    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("select `c3`, sum(`alias_c1`) as `sum c1`, sum(`mean`) as `sum GT` from (select `c3`, `c1` AS `alias_c1`, avg(`c2`) as `mean` from `a` group by `c3`, `alias_c1`) where `type` = 'myType' group by `c3`");
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
    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("select (sum(`pnl`)+avg(`delta`)) as `plus`, ((sum(`pnl`)+avg(`delta`))/100.0) as `divide` from `a`");
  }

  @Test
  void testAggregatedMeasures() {
    TableDto a = new TableDto(BASE_STORE_NAME);
    QueryDto query = new QueryDto()
            .table(a)
            .withMeasure(sum("pnlSum", "pnl"))
            .withMeasure(sumIf("pnlSumFiltered", "pnl", criterion("country", eq("france"))));
    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("select sum(`pnl`) as `pnlSum`, sum(case when `country` = 'france' then `pnl` end) as `pnlSumFiltered` from `" + BASE_STORE_NAME + "`");

    // With full path
    query = new QueryDto()
            .table(a)
            .withMeasure(sum("pnlSum", a.name + ".pnl"))
            .withMeasure(sumIf("pnlSumFiltered", a.name + ".pnl", criterion(a.name + ".country", eq("france"))));
    String format = "select sum(`%1$s`.`pnl`) as `pnlSum`, sum(case when `%1$s`.`country` = 'france' then `%1$s`.`pnl` end) as `pnlSumFiltered` from `%1$s`";
    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo(String.format(
                    format, BASE_STORE_NAME));
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
    String expected = String.format("with %2$s as (select 0 as `a`, '0' as `b` union all select 1 as `a`, '1' as `b`) " +
                    "select `id`, `b`, sum(`pnl`) as `pnl.sum` from `%1$s` " +
                    "inner join %2$s on `id` = `a` group by `id`, `b`",
            BASE_STORE_NAME, virtualTable.name);
    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo(expected);
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

    String expected = String.format("with %2$s as (select 0 as `a`, '0' as `b` union all select 1 as `a`, '1' as `b`) " +
                    "select `id`, `b`, sum(`pnl`) as `pnl.sum` from `%1$s` " +
                    "inner join %2$s on `%1$s`.`id` = %2$s.`a` group by `id`, `b`",
            BASE_STORE_NAME, virtualTable.name);
    // IMPORTANT: No backtick in the join condition for the field belonging to the virtual table.
    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo(expected);
  }

  @Test
  void testGroupingSets() {
    final Field a = tableField("a");
    final Field b = tableField("b");
    final QueryDto query = new QueryDto()
            .withColumn(a)
            .withColumn(b)
            .withMeasure(new AggregatedMeasure("pnl.sum", "pnl", "sum"))
            .table(BASE_STORE_NAME);
    query.groupingSets = List.of(
            List.of(), // GT
            List.of(a), // total a
            List.of(a, b));
    Assertions.assertThat(translate(compileQuery(query)))
            .isEqualTo("select `a`, `b`, sum(`pnl`) as `pnl.sum` from `baseStore` group by grouping sets((), (`a`), (`a`,`b`))");
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
    Assertions.assertThat(expression)
            .isEqualTo("sum(case when (`recommendation`.`finalprice` > `recommendation`.`recoprice` and `recommendation`.`recoprice` > '0')" +
                    " then (`recommendation`.`finalprice`-`recommendation`.`recoprice`) end)" +
                    " as `increase_sum`");

    Field initial_price = new TableField("recommendation.initial_price");
    Field one = new ConstantField(1);
    Field tvaRate = new TableField("product.tva_rate");
    Measure whatever = sum("whatever", divide(initial_price, plus(one, tvaRate)));
    Assertions.assertThat(compiledMeasure(whatever).sqlExpression(qr, true))
            .isEqualTo("sum((`recommendation`.`initial_price`/(1+`product`.`tva_rate`))) as `whatever`");
  }

  @Test
  void testComplexFieldCalculation() {
    DefaultQueryRewriter qr = new DefaultQueryRewriter(null);
    Field f1 = new TableField("f1");
    Field f2 = new TableField("f2");
    Field f3 = new TableField("f3");

    BinaryOperationField f1_minus_f2 = new BinaryOperationField(BinaryOperator.MINUS, f1, f2);
    BinaryOperationField divide = new BinaryOperationField(BinaryOperator.DIVIDE, f1_minus_f2, f1);
    BinaryOperationField multiply = new BinaryOperationField(BinaryOperator.MULTIPLY, divide, f3);
    BinaryOperationField plus = new BinaryOperationField(BinaryOperator.PLUS, multiply, new ConstantField(2));
    Assertions.assertThat(compileField(plus).sqlExpression(qr))
            .isEqualTo("((((`f1`-`f2`)/`f1`)*`f3`)+2)");
  }
}
