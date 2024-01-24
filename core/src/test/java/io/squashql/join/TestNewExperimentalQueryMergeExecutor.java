package io.squashql.join;

import io.squashql.query.TableField;
import io.squashql.query.builder.Query;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.database.*;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.join.JoinQuery;
import io.squashql.query.join.NewExperimentalQueryMergeExecutor;
import io.squashql.store.Datastore;
import io.squashql.store.Store;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;
import org.assertj.core.api.Assertions;
import org.eclipse.collections.api.tuple.Triple;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.squashql.query.Functions.all;
import static io.squashql.query.Functions.criterion;
import static io.squashql.query.dto.ConditionType.EQ;

/**
 * Test sql generation, not the execution. See {@link io.squashql.query.ATestExperimentalQueryResultMerge}.
 */
public class TestNewExperimentalQueryMergeExecutor {

  static final String storeA = "A"; // + getClass().getSimpleName().toLowerCase();
  static final String storeB = "B"; // + getClass().getSimpleName().toLowerCase();
  static final String storeC = "C"; // + getClass().getSimpleName().toLowerCase();

  static final TableField A_a = new TableField(storeA, "a");
  static final TableField A_b = new TableField(storeA, "b");
  static final TableField A_c = new TableField(storeA, "c");

  static final TableField B_b = new TableField(storeB, "b");
  static final TableField B_d = new TableField(storeB, "d");
  static final TableField B_e = new TableField(storeB, "e");
  static final TableField B_f = new TableField(storeB, "f");

  static final TableField C_b = new TableField(storeC, "b");
  static final TableField C_c = new TableField(storeC, "c");
  static final TableField C_d = new TableField(storeC, "d");
  static final TableField C_g = new TableField(storeC, "g");

  static final NewExperimentalQueryMergeExecutor ex = new NewExperimentalQueryMergeExecutor(new FakeQueryEngine());

  static class FakeQueryEngine implements QueryEngine<Datastore> {

    @Override
    public Table execute(DatabaseQuery query) {
      return null;
    }

    @Override
    public Table executeRawSql(String sql) {
      return null;
    }

    @Override
    public Datastore datastore() {
      return () -> {
        Map<String, Store> map = new HashMap<>();
        try {
          java.lang.reflect.Field[] fields = TestNewExperimentalQueryMergeExecutor.class.getDeclaredFields();
          for (java.lang.reflect.Field field : fields) {
            Class<?> type = field.getType();
            if (type.equals(TableField.class)) {
              TableField f = (TableField) field.get(null);
              map.computeIfAbsent(f.tableName, k -> new Store(f.tableName, new ArrayList<>()))
                      .fields()
                      .add(new TableTypedField(f.tableName, f.fieldName, Object.class));
            }
          }
          return map;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };
    }

    @Override
    public List<String> supportedAggregationFunctions() {
      return null;
    }

    @Override
    public QueryRewriter queryRewriter(DatabaseQuery query) {
      return new DefaultQueryRewriter(query) {

        static final Pattern p = Pattern.compile("__cte([0-9])__");

        @Override
        public String cteName(String cteName) {
          Matcher matcher = p.matcher(cteName);
          if (matcher.find()) {
            return super.cteName("CTE" + matcher.group(1).toUpperCase()); // to make sure it differs from tableName()
          } else {
            throw new RuntimeException("incorrect cteName");
          }
        }
      };
    }
  }

  @Test
  void testCorrectColumnsAreKeptInSelectFromLeftToRight() {
    QueryDto queryA = Query
            .from(storeA)
            .select(List.of(A_a, A_b, A_c), List.of())
            .build();

    QueryDto queryB = Query
            .from(storeB)
            .select(List.of(B_b, B_d, B_e, B_f), List.of())
            .build();

    QueryDto queryC = Query
            .from(storeC)
            .select(List.of(C_b, C_c, C_d, C_g), List.of())
            .build();

    JoinQuery joinQuery = JoinQuery.from(queryA)
            .join(queryB, JoinType.LEFT, criterion(A_b, B_b, EQ))
            .join(queryC, JoinType.LEFT, all(
                    criterion(A_b, C_b, EQ),
                    criterion(A_c, C_c, EQ),
                    criterion(B_d, C_d, EQ)
            ));

    Triple<String, List<TypedField>, List<CompiledMeasure>> result = ex.generateSql(joinQuery, null, 12);
    String expectedSql = "with `CTE0` as (" +
            "select `A`.`a`, `A`.`b`, `A`.`c` from `A` group by `A`.`a`, `A`.`b`, `A`.`c`), " +
            "`CTE1` as (" +
            "select `B`.`b`, `B`.`d`, `B`.`e`, `B`.`f` from `B` group by `B`.`b`, `B`.`d`, `B`.`e`, `B`.`f`), " +
            "`CTE2` as (" +
            "select `C`.`b`, `C`.`c`, `C`.`d`, `C`.`g` from `C` group by `C`.`b`, `C`.`c`, `C`.`d`, `C`.`g`) " +
            "select `CTE0`.`a`, `CTE0`.`b`, `CTE0`.`c`, `CTE1`.`d`, `CTE1`.`e`, `CTE1`.`f`, `CTE2`.`g` " +
            "from `CTE0` left join `CTE1` on `CTE0`.`b` = `CTE1`.`b` " +
            "left join `CTE2` on (`CTE0`.`b` = `CTE2`.`b` and `CTE0`.`c` = `CTE2`.`c` and `CTE1`.`d` = `CTE2`.`d`) " +
            "limit 12";
    Assertions.assertThat(result.getOne()).isEqualTo(expectedSql);
    List<String> selectElements = result.getTwo().stream().map(SqlUtils::squashqlExpression).toList();
    Assertions.assertThat(selectElements).containsExactly("A.a", "A.b", "A.c", "B.d", "B.e", "B.f", "C.g");
  }

  @Test
  void testNoColumnInCommonWithoutCondition() {
    QueryDto queryA = Query
            .from(storeA)
            .select(List.of(A_a, A_b, A_c), List.of())
            .build();

    QueryDto queryB = Query
            .from(storeB)
            .select(List.of(B_b, B_d, B_e, B_f), List.of())
            .build();

    JoinQuery joinQuery = JoinQuery.from(queryA)
            .join(queryB, JoinType.CROSS);

    Triple<String, List<TypedField>, List<CompiledMeasure>> result = ex.generateSql(joinQuery, null, 12);
    String expectedSql = "with `CTE0` as (" +
            "select `A`.`a`, `A`.`b`, `A`.`c` from `A` group by `A`.`a`, `A`.`b`, `A`.`c`), " +
            "`CTE1` as (" +
            "select `B`.`b`, `B`.`d`, `B`.`e`, `B`.`f` from `B` group by `B`.`b`, `B`.`d`, `B`.`e`, `B`.`f`) " +
            "select `CTE0`.`a`, `CTE0`.`b`, `CTE0`.`c`, `CTE1`.`b`, `CTE1`.`d`, `CTE1`.`e`, `CTE1`.`f` " +
            "from `CTE0` cross join `CTE1` " +
            "limit 12";
    Assertions.assertThat(result.getOne()).isEqualTo(expectedSql);
    List<String> selectElements = result.getTwo().stream().map(SqlUtils::squashqlExpression).toList();
    Assertions.assertThat(selectElements).containsExactly("A.a", "A.b", "A.c", "B.b", "B.d", "B.e", "B.f");
  }

  @Test
  void testColumnInCommonWithAliasWithoutCondition() {
    QueryDto queryA = Query
            .from(storeA)
            .select(List.of(A_a, A_b.as("alias_b"), A_c), List.of())
            .build();

    QueryDto queryB = Query
            .from(storeB)
            .select(List.of(B_b.as("alias_b"), B_d, B_e, B_f), List.of())
            .build();

    // Should be a condition on "alias_b"
    JoinQuery joinQuery = JoinQuery.from(queryA)
            .join(queryB, JoinType.LEFT);

    Triple<String, List<TypedField>, List<CompiledMeasure>> result = ex.generateSql(joinQuery, null, 12);
    String expectedSql = "with `CTE0` as (" +
            "select `A`.`a`, `A`.`b` as `alias_b`, `A`.`c` from `A` group by `A`.`a`, `alias_b`, `A`.`c`), " +
            "`CTE1` as (" +
            "select `B`.`b` as `alias_b`, `B`.`d`, `B`.`e`, `B`.`f` from `B` group by `alias_b`, `B`.`d`, `B`.`e`, `B`.`f`) " +
            "select `CTE0`.`a`, `CTE0`.`alias_b` as `alias_b`, `CTE0`.`c`, `CTE1`.`d`, `CTE1`.`e`, `CTE1`.`f` " +
            "from `CTE0` left join `CTE1` on `CTE0`.`alias_b` = `CTE1`.`alias_b` " +
            "limit 12";
    Assertions.assertThat(result.getOne()).isEqualTo(expectedSql);
    List<String> selectElements = result.getTwo().stream().map(SqlUtils::squashqlExpression).toList();
    Assertions.assertThat(selectElements).containsExactly("A.a", "alias_b", "A.c", "B.d", "B.e", "B.f");
  }

  @Test
  void testColumnInCommonWithSameNameWithoutCondition() {
    QueryDto queryA = Query
            .from(storeA)
            .select(List.of(A_a, A_b, A_c), List.of())
            .build();

    QueryDto queryB = Query
            .from(storeA)
            .select(List.of(A_b, A_c), List.of())
            .build();

    // Should be a condition on "alias_b"
    JoinQuery joinQuery = JoinQuery.from(queryA)
            .join(queryB, JoinType.LEFT);

    Triple<String, List<TypedField>, List<CompiledMeasure>> result = ex.generateSql(joinQuery, null, 12);
    String expectedSql = "with `CTE0` as (" +
            "select `A`.`a`, `A`.`b`, `A`.`c` from `A` group by `A`.`a`, `A`.`b`, `A`.`c`), " +
            "`CTE1` as (" +
            "select `A`.`b`, `A`.`c` from `A` group by `A`.`b`, `A`.`c`) " +
            "select `CTE0`.`a`, `CTE0`.`b`, `CTE0`.`c` " +
            "from `CTE0` left join `CTE1` on (`CTE0`.`b` = `CTE1`.`b` and `CTE0`.`c` = `CTE1`.`c`) " +
            "limit 12";
    Assertions.assertThat(result.getOne()).isEqualTo(expectedSql);
    List<String> selectElements = result.getTwo().stream().map(SqlUtils::squashqlExpression).toList();
    Assertions.assertThat(selectElements).containsExactly("A.a", "A.b", "A.c");
  }

  @Test
  void testColumnInCommonWithSameNameAndWithCondition() {
    QueryDto queryA = Query
            .from(storeA)
            .select(List.of(A_a, A_b.as("alias_b"), A_c), List.of())
            .build();

    QueryDto queryB = Query
            .from(storeB)
            .select(List.of(B_b.as("alias_b"), B_d, B_e, B_f), List.of())
            .build();

    QueryDto queryC = Query
            .from(storeC)
            .select(List.of(C_b, C_c, C_d, C_g), List.of())
            .build();

    JoinQuery joinQuery = JoinQuery.from(queryA)
            .join(queryB, JoinType.LEFT) // No condition on query, it should guess the condition on "alias_b"
            .join(queryC, JoinType.LEFT, all(
                    criterion(A_b, C_b, EQ),
                    criterion(A_c, C_c, EQ),
                    criterion(B_d, C_d, EQ)
            ));

    Triple<String, List<TypedField>, List<CompiledMeasure>> result = ex.generateSql(joinQuery, null, 12);
    String expectedSql = "with `CTE0` as (" +
            "select `A`.`a`, `A`.`b` as `alias_b`, `A`.`c` from `A` group by `A`.`a`, `alias_b`, `A`.`c`), " +
            "`CTE1` as (" +
            "select `B`.`b` as `alias_b`, `B`.`d`, `B`.`e`, `B`.`f` from `B` group by `alias_b`, `B`.`d`, `B`.`e`, `B`.`f`), " +
            "`CTE2` as (" +
            "select `C`.`b`, `C`.`c`, `C`.`d`, `C`.`g` from `C` group by `C`.`b`, `C`.`c`, `C`.`d`, `C`.`g`) " +
            "select `CTE0`.`a`, `CTE0`.`alias_b` as `alias_b`, `CTE0`.`c`, `CTE1`.`d`, `CTE1`.`e`, `CTE1`.`f`, `CTE2`.`g` " +
            "from `CTE0` " +
            "left join `CTE1` on `CTE0`.`alias_b` = `CTE1`.`alias_b` " +
            "left join `CTE2` on (`CTE0`.`b` = `CTE2`.`b` and `CTE0`.`c` = `CTE2`.`c` and `CTE1`.`d` = `CTE2`.`d`) " +
            "limit 12";
    Assertions.assertThat(result.getOne()).isEqualTo(expectedSql);
    List<String> selectElements = result.getTwo().stream().map(SqlUtils::squashqlExpression).toList();
    Assertions.assertThat(selectElements).containsExactly("A.a", "alias_b", "A.c", "B.d", "B.e", "B.f", "C.g");
  }
}
