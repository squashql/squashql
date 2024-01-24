package io.squashql.join;

import io.squashql.query.TableField;
import io.squashql.query.builder.Query;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.DefaultQueryRewriter;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.QueryRewriter;
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
        @Override
        public String cteName(String cteName) {
          return super.cteName(cteName + "c"); // to make sure it differs from tableName()
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

    Triple<String, List<TypedField>, List<CompiledMeasure>> result = new NewExperimentalQueryMergeExecutor(new FakeQueryEngine())
            .generateSql(joinQuery, null, 12);
    String expectedSql = "with `__cte0__c` as (" +
            "select `A`.`a`, `A`.`b`, `A`.`c` from `A` group by `A`.`a`, `A`.`b`, `A`.`c`), `__cte1__c` as (" +
            "select `B`.`b`, `B`.`d`, `B`.`e`, `B`.`f` from `B` group by `B`.`b`, `B`.`d`, `B`.`e`, `B`.`f`), `__cte2__c` as (" +
            "select `C`.`b`, `C`.`c`, `C`.`d`, `C`.`g` from `C` group by `C`.`b`, `C`.`c`, `C`.`d`, `C`.`g`) " +
            "select `__cte0__c`.`a`, `__cte0__c`.`b`, `__cte0__c`.`c`, `__cte1__c`.`d`, `__cte1__c`.`e`, `__cte1__c`.`f`, `__cte2__c`.`g` " +
            "from `__cte0__c` left join `__cte1__c` on `__cte0__c`.`b` = `__cte1__c`.`b` " +
            "left join `__cte2__c` on (`__cte0__c`.`b` = `__cte2__c`.`b` and `__cte0__c`.`c` = `__cte2__c`.`c` and `__cte1__c`.`d` = `__cte2__c`.`d`) " +
            "limit 12";
    Assertions.assertThat(result.getOne()).isEqualTo(expectedSql);
    List<String> selectElements = result.getTwo().stream().map(f -> ((TableTypedField) f).store() + "." + f.name()).toList();
    Assertions.assertThat(selectElements).containsExactly("A.a", "A.b", "A.c", "B.d", "B.e", "B.f", "C.g");
  }
}
