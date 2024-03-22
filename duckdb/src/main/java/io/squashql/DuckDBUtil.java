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
  public static Class<?> getColumnJavaClass(ResultSetMetaData metaData, int column) {
    try {
      return sqlTypeToJavaClass(metaData.getColumnType(column), metaData.getColumnTypeName(column));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static Class<?> sqlTypeToJavaClass(int dataType, String columnTypeName) {
    Class<?> klass = JdbcUtil.sqlTypeToClass(dataType);
    // Special case for DuckDBColumnType.HUGEINT (128 bits integer). See also #getTypeValue
    if (klass == Object.class) {
      if (columnTypeName.equals(DuckDBColumnType.HUGEINT.name())) {
        klass = long.class;
      }
    } else if (klass == List.class) {
      if (columnTypeName.equals("DOUBLE[]") || columnTypeName.equals("FLOAT[]")) {
        return Lists.DoubleList.class;
      } else if (columnTypeName.equals("LONG[]") || columnTypeName.equals("HUGEINT[]") || columnTypeName.equals("INT[]") || columnTypeName.equals("INTEGER[]")) {
        return Lists.LongList.class;
      } else if (columnTypeName.equals("DATE[]")) {
        return Lists.LocalDateList.class;
      } else if (columnTypeName.equals("VARCHAR[]")) {
        return Lists.StringList.class;
      } else {
        return List.class; // we convert Array to List
      }
    }
    return klass;
  }
}
