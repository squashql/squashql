package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.list.Lists;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static io.squashql.query.Functions.*;

@TestClass(ignore = TestClass.Type.SNOWFLAKE)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestVector extends ABaseTestQuery {
  final String storeName = "mystore" + getClass().getSimpleName().toLowerCase();
  final Field tag = new TableField(this.storeName, "tag");
  final Field tagSt = new TableField(this.storeName, "tagSt");
  final Field ean = new TableField(this.storeName, "ean");
  final Field price = new TableField(this.storeName, "price");

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    TableTypedField tag = new TableTypedField(this.storeName, "tag", Lists.LongList.class);
    TableTypedField tagSt = new TableTypedField(this.storeName, "tagSt", Lists.StringList.class);
    return Map.of(this.storeName, List.of(ean, price, tag, tagSt));
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeName, List.of(
            new Object[]{"A", 10d, List.of(1L, 3L, 5L), List.of("1", "3", "5")},
            new Object[]{"B", 8d, List.of(1L, 2L), List.of("1", "2")},
            new Object[]{"C", 2d, List.of(2L, 4L, 6L), List.of("2", "4", "6")},
            new Object[]{"D", 1d, List.of(1L), List.of("1")}
    ));
  }

  @Test
  void testArrayContainsNumber() {
    QueryDto query = Query
            .from(this.storeName)
            .where(criterion(this.tag, contains(2)))
            .select(List.of(this.ean), List.of(sum("price", this.price)))
            .build();
    Table table = this.executor.executeQuery(query);
    Assertions.assertThat(table).containsExactly(
            List.of("B", 8d),
            List.of("C", 2d));
  }

  @Test
  void testArrayContainsString() {
    QueryDto query = Query
            .from(this.storeName)
            .where(criterion(this.tagSt, contains("2")))
            .select(List.of(this.ean, this.tagSt), List.of(sum("price", this.price)))
            .build();
    Table table = this.executor.executeQuery(query);
    Assertions.assertThat(table).containsExactly(
            List.of("B", 8d),
            List.of("C", 2d));
  }
}
