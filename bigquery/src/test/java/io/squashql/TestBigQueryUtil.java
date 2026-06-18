package io.squashql;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.time.LocalDate;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

class TestBigQueryUtil {

  @Test
  void testBigQueryTypeToClass() {
    assertThat(BigQueryUtil.bigQueryTypeToClass(field(StandardSQLTypeName.BOOL))).isEqualTo(boolean.class);
    assertThat(BigQueryUtil.bigQueryTypeToClass(field(StandardSQLTypeName.INT64))).isEqualTo(long.class);
    assertThat(BigQueryUtil.bigQueryTypeToClass(field(StandardSQLTypeName.FLOAT64))).isEqualTo(double.class);
    assertThat(BigQueryUtil.bigQueryTypeToClass(field(StandardSQLTypeName.STRING))).isEqualTo(String.class);
    assertThat(BigQueryUtil.bigQueryTypeToClass(field(StandardSQLTypeName.BYTES))).isEqualTo(byte.class);
    assertThat(BigQueryUtil.bigQueryTypeToClass(field(StandardSQLTypeName.DATE))).isEqualTo(LocalDate.class);
    assertThat(BigQueryUtil.bigQueryTypeToClass(field(StandardSQLTypeName.DATETIME))).isEqualTo(LocalDateTime.class);
  }

  /**
   * NUMERIC and BIGNUMERIC must be mapped to a numeric type (double) and not fall back to {@link Object}, otherwise
   * their values are read as String (see {@code BigQueryEngine#getTypeValue}), which is inconsistent with the
   * PostgreSQL engine that exposes numeric columns as numbers.
   */
  @Test
  void testBigQueryTypeToClassForNumeric() {
    assertThat(BigQueryUtil.bigQueryTypeToClass(field(StandardSQLTypeName.NUMERIC))).isEqualTo(double.class);
    assertThat(BigQueryUtil.bigQueryTypeToClass(field(StandardSQLTypeName.BIGNUMERIC))).isEqualTo(double.class);
  }

  private static Field field(StandardSQLTypeName type) {
    return Field.of("f", type);
  }
}
