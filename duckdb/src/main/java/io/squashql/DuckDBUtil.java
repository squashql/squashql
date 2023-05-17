package io.squashql;

import io.squashql.jdbc.JdbcUtil;
import org.duckdb.DuckDBColumnType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

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
      }
      return klass;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
