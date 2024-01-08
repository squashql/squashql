package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.list.Lists;
import io.squashql.query.builder.Query;
import io.squashql.query.dictionary.ObjectArrayDictionary;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.squashql.query.agg.AggregationFunction.SUM;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestVectorAggregationDataType extends ABaseTestQuery {

  static String mmm = "MMM";
  static String vblax = "VBLAX";
  static LocalDate d1 = LocalDate.of(2023, 1, 1);
  static LocalDate d2 = LocalDate.of(2023, 1, 2);
  static LocalDate d3 = LocalDate.of(2023, 1, 3);
  static LocalDate d4 = LocalDate.of(2023, 1, 4);
  String storeName = "store" + getClass().getSimpleName().toLowerCase() + System.currentTimeMillis();
//  String storeName = "store" + getClass().getSimpleName().toLowerCase(); // FIXME

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField ticker = new TableTypedField(this.storeName, "ticker", String.class);
    TableTypedField date = new TableTypedField(this.storeName, "date", LocalDate.class);
    TableTypedField vD = new TableTypedField(this.storeName, "valueDouble", double.class);
    TableTypedField vF = new TableTypedField(this.storeName, "valueFloat", float.class);
    TableTypedField vL = new TableTypedField(this.storeName, "valueLong", long.class);
    TableTypedField vI = new TableTypedField(this.storeName, "valueInt", int.class);
    return Map.of(this.storeName, List.of(ticker, date, vD, vF, vL, vI));
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeName, List.of(
            new Object[]{mmm, d1, 1d, 1f, 1L, 1},
            new Object[]{mmm, d2, 1.5d, 1.5f, 5L, 5},
            new Object[]{mmm, d3, 1.6d, 1.6f, 6L, 6},
            new Object[]{mmm, d4, 1.7d, 1.7f, 7L, 7},

            new Object[]{vblax, d1, 2d, 2f, 2L, 2},
            new Object[]{vblax, d2, 1.8d, 1.8f, 8L, 8},
            new Object[]{vblax, d3, 1.9d, 1.9f, 9L, 9}
    ));
  }

  @ParameterizedTest
  @ValueSource(strings = {"Int", "Long", "Float", "Double"})
  void testVectorType(String type) {
    assertVectorType(type);
  }

  void assertVectorType(String type) {
    Map<String, List<? extends List<? extends Number>>> expectedVectorsByType = Map.of(
            "Int", List.of(List.of(1L, 5L, 6L, 7L), List.of(8L, 9L, 2L)),
            "Long", List.of(List.of(1L, 5L, 6L, 7L), List.of(8L, 9L, 2L)),
            "Float", List.of(List.of(1.7d, 1.6d, 1.0d, 1.5d), List.of(1.8d, 1.9d, 2.0d)),
            "Double", List.of(List.of(1.7d, 1.6d, 1.0d, 1.5d), List.of(1.8d, 1.9d, 2.0d)));

    Field ticker = new TableField(this.storeName, "ticker");
    Field valueType = new TableField(this.storeName, "value" + type);
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", valueType, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ticker), List.of(vector))
            .build();
    Table result = this.executor.executeQuery(query);
    List<List<Object>> points = List.of(List.of(mmm), List.of(vblax));
    assertVectorValues((ColumnarTable) result, vector, points, (List<List<Number>>) expectedVectorsByType.get(type), type);
  }

  private void assertVectorValues(ColumnarTable result, Measure vectorMeasure, List<List<Object>> points, List<List<Number>> expectedVectors, String type) {
    Header header = result.getHeader(vectorMeasure.alias());
    Class<?> expectedType = null;
    if (type.equals("Int") || type.equals("Long")) {
      expectedType = Lists.LongList.class;
    } else if (type.equals("Double") || type.equals("Float")) {
      expectedType = Lists.DoubleList.class;
    } else {
      Assertions.fail("Unknown type " + type);
    }
    Assertions.assertThat(header.type()).isEqualTo(expectedType);
    List<Object> aggregateValues = result.getColumnValues(vectorMeasure.alias());
    for (int i = 0; i < points.size(); i++) {
      ObjectArrayDictionary dictionary = result.pointDictionary.get();
      int position = dictionary.getPosition(points.get(i).toArray());
      List<?> actual = (List<?>) aggregateValues.get(position);
      // SORT to have a deterministic comparison
      List<Number> vector = new ArrayList<>(expectedVectors.get(i)).stream().sorted().toList();
      List<Number> actualVector = (List<Number>) new ArrayList<>(actual).stream().sorted().toList();

      if (type.equals("Int") || type.equals("Long")) {
        Assertions.assertThat(actual).isInstanceOf(expectedType);
      } else if (type.equals("Double") || type.equals("Float")) {
        Assertions.assertThat(actual).isInstanceOf(expectedType);
      } else {
        Assertions.fail("Unknown type " + type);
      }

      if (header.type() == Lists.DoubleList.class) {
        int size = actualVector.size();
        Assertions.assertThat(vector.size()).isEqualTo(size);
        for (int j = 0; j < size; j++) {
          Assertions.assertThat((Double) vector.get(j)).isCloseTo((Double) actualVector.get(j), Offset.offset(0.001d));
        }
      } else {
        Assertions.assertThat(actualVector).containsExactlyElementsOf(vector);
      }
    }
  }
}
