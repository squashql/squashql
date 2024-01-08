package io.squashql;

import io.squashql.jdbc.JdbcUtil;
import io.squashql.list.Lists;
import org.duckdb.DuckDBColumnType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public final class DuckDBUtil {

  private DuckDBUtil() {
  }

  /**
   * Specialized {@link JdbcUtil#sqlTypeToClass(int)} to handle specific DuckDB cases like {@link DuckDBColumnType#HUGEINT}.
   *
   * @param metaData {@link ResultSetMetaData}
   * @param column   the first column is 1, the second is 2, ...
   * @return the equivalent java class.
   */
  public static Class<?> sqlTypeToClass(ResultSetMetaData metaData, int column) {
    try {
      Class<?> klass = JdbcUtil.sqlTypeToClass(metaData.getColumnType(column));
      // Special case for DuckDBColumnType.HUGEINT (128 bits integer). See also #getTypeValue
      if (klass == Object.class) {
        String columnTypeName = metaData.getColumnTypeName(column);
        if (columnTypeName.equals(DuckDBColumnType.HUGEINT.name())) {
          klass = long.class;
        }
      } else if (klass == List.class) {
        String typeName = metaData.getColumnTypeName(column);
        if (typeName.equals("DOUBLE[]") || typeName.equals("FLOAT[]")) {
          return Lists.DoubleList.class;
        } else if (typeName.equals("LONG[]") || typeName.equals("HUGEINT[]") || typeName.equals("INT[]")) {
          return Lists.LongList.class;
        } else {
          return List.class; // we convert Array to List
        }
      }
      return klass;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
