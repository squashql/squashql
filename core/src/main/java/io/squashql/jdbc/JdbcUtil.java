package io.squashql.jdbc;

import io.squashql.list.Lists;
import io.squashql.query.Header;
import io.squashql.store.Store;
import io.squashql.table.RowTable;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class JdbcUtil {

  private JdbcUtil() {
  }

  public static String classToSqlType(Class<?> clazz) {
    if (clazz.equals(String.class)) {
      return "STRING";
    }
    if (clazz.equals(Byte.class) || clazz.equals(byte.class)) {
      return "TINYINT";
    }
    if (clazz.equals(Short.class) || clazz.equals(short.class)) {
      return "SMALLINT";
    }
    if (clazz.equals(Integer.class) || clazz.equals(int.class)) {
      return "INTEGER";
    }
    if (clazz.equals(Long.class) || clazz.equals(long.class)) {
      return "BIGINT";
    }
    if (clazz.equals(Float.class) || clazz.equals(float.class)) {
      return "FLOAT";
    }
    if (clazz.equals(Double.class) || clazz.equals(double.class)) {
      return "DOUBLE";
    }
    if (clazz.equals(Boolean.class) || clazz.equals(boolean.class)) {
      return "BOOLEAN";
    }
    if (clazz.equals(java.sql.Date.class) || clazz.equals(java.time.LocalDate.class)) {
      return "DATE";
    }
    if (clazz.equals(java.sql.Time.class) || clazz.equals(java.time.LocalDateTime.class)) {
      return "TIME";
    }
    if (clazz.equals(java.sql.Timestamp.class)) {
      return "TIMESTAMP";
    }
    if (clazz.equals(Lists.LongList.class)) {
      return "INT[]"; // FIXME duckdb only
    }
    if (clazz.equals(Lists.StringList.class)) {
      return "VARCHAR[]"; // FIXME duckdb only
    }
    if (clazz.equals(Object.class)) {
      return "STRUCT";
    }
    throw new IllegalArgumentException("Unsupported field type " + clazz);
  }

  public static Class<?> sqlTypeToClass(int dataType) {
    return switch (dataType) {
      case Types.CHAR, Types.NVARCHAR, Types.VARCHAR, Types.LONGVARCHAR -> String.class;
      case Types.BOOLEAN, Types.BIT -> boolean.class;
      case Types.TINYINT -> byte.class;
      case Types.SMALLINT -> short.class;
      case Types.INTEGER -> int.class;
      case Types.BIGINT -> long.class;
      case Types.REAL, Types.FLOAT -> float.class;
      case Types.DECIMAL, Types.DOUBLE -> double.class;
      case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> byte[].class;
      case Types.DATE -> LocalDate.class;
      case Types.TIME -> LocalDateTime.class;
      case Types.TIMESTAMP -> Timestamp.class;
      case Types.ARRAY -> List.class;
      case Types.NUMERIC -> Number.class;
      default -> Object.class;
    };
  }

  /**
   * Example of the {@link ResultSet}.
   * <pre>
   * +-----------+-------------+------------------------------+-------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------+
   * | TABLE_CAT | TABLE_SCHEM |                   TABLE_NAME | COLUMN_NAME | DATA_TYPE | TYPE_NAME | COLUMN_SIZE | BUFFER_LENGTH | DECIMAL_DIGITS | NUM_PREC_RADIX | NULLABLE | REMARKS | COLUMN_DEF | SQL_DATA_TYPE | SQL_DATETIME_SUB | CHAR_OCTET_LENGTH | ORDINAL_POSITION | IS_NULLABLE | SCOPE_CATALOG | SCOPE_SCHEMA | SCOPE_TABLE | SOURCE_DATA_TYPE | IS_AUTOINCREMENT | IS_GENERATEDCOLUMN |
   * +-----------+-------------+------------------------------+-------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------+
   * |    memory |        main | storetestduckdbqueryexecutor |       eanId |         4 |   INTEGER |        null |          null |             32 |             10 |        1 |    null |       null |          null |             null |              null |                1 |         YES |          null |         null |        null |             null |                  |                    |
   * |    memory |        main | storetestduckdbqueryexecutor |         ean |        12 |   VARCHAR |        null |          null |           null |             10 |        1 |    null |       null |          null |             null |              null |                2 |         YES |          null |         null |        null |             null |                  |                    |
   * |    memory |        main | storetestduckdbqueryexecutor |    category |        12 |   VARCHAR |        null |          null |           null |             10 |        1 |    null |       null |          null |             null |              null |                3 |         YES |          null |         null |        null |             null |                  |                    |
   * |    memory |        main | storetestduckdbqueryexecutor | subcategory |        12 |   VARCHAR |        null |          null |           null |             10 |        1 |    null |       null |          null |             null |              null |                4 |         YES |          null |         null |        null |             null |                  |                    |
   * |    memory |        main | storetestduckdbqueryexecutor |       price |         8 |    DOUBLE |        null |          null |             53 |             10 |        1 |    null |       null |          null |             null |              null |                5 |         YES |          null |         null |        null |             null |                  |                    |
   * |    memory |        main | storetestduckdbqueryexecutor |    quantity |         4 |   INTEGER |        null |          null |             32 |             10 |        1 |    null |       null |          null |             null |              null |                6 |         YES |          null |         null |        null |             null |                  |                    |
   * |    memory |        main | storetestduckdbqueryexecutor |      isFood |        16 |   BOOLEAN |        null |          null |           null |             10 |        1 |    null |       null |          null |             null |              null |                7 |         YES |          null |         null |        null |             null |                  |                    |
   * |    memory |        main | storetestduckdbqueryexecutor |    scenario |        12 |   VARCHAR |        null |          null |           null |             10 |        1 |    null |       null |          null |             null |              null |                8 |         YES |          null |         null |        null |             null |                  |                    |
   * +-----------+-------------+------------------------------+-------------+-----------+-----------+-------------+---------------+----------------+----------------+----------+---------+------------+---------------+------------------+-------------------+------------------+-------------+---------------+--------------+-------------+------------------+------------------+--------------------+
   * </pre>
   */
  public static Map<String, Store> getStores(String catalog, String schema, Connection connection, BiFunction<Integer, String, Class<?>> typeToClass) {
    try (connection) {
      DatabaseMetaData metadata = connection.getMetaData();
      Map<String, Store> stores = new HashMap<>();
      ResultSet resultSet = metadata.getColumns(catalog, schema, "%", null);
      while (resultSet.next()) {
        String tableName = resultSet.getString("TABLE_NAME");
        String columnName = resultSet.getString("COLUMN_NAME");
        int dataType = resultSet.getInt("DATA_TYPE");
        String columnTypeName = resultSet.getString("TYPE_NAME");
        stores.computeIfAbsent(tableName, k -> new Store(k, new ArrayList<>()))
                .fields()
                .add(new TableTypedField(tableName, columnName, typeToClass.apply(dataType, columnTypeName)));
      }
      return stores;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }


  public static void show(ResultSet set) {
    StringBuilder sb = new StringBuilder();
    try {
      ResultSetMetaData metaData = set.getMetaData();
      int columnCount = metaData.getColumnCount();

      for (int i = 0; i < columnCount; i++) {
        sb.append(metaData.getColumnName(i + 1));
      }
      while (set.next()) {
        for (int i = 0; i < columnCount; i++) {
          sb.append(set.getObject(i + 1)).append(",");
        }
        sb.append(System.lineSeparator());
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    System.out.println(sb);
  }

  /**
   * DO NOT USE in prod. This is for debugging purpose.
   */
  public static Table toTable(ResultSet resultSet) {
    try {
      List<Header> headers = createHeaderList(resultSet);
      List<List<Object>> rows = new ArrayList<>();
      while (resultSet.next()) {
        rows.add(IntStream.range(0, headers.size()).mapToObj(i -> {
          try {
            return resultSet.getObject(i + 1);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }).toList());
      }
      return new RowTable(headers, rows);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<Header> createHeaderList(ResultSet tableResult) throws SQLException {
    List<Header> headers = new ArrayList<>();
    ResultSetMetaData metadata = tableResult.getMetaData();
    // get the column names; column indexes start from 1
    for (int i = 1; i < metadata.getColumnCount() + 1; i++) {
      String fieldName = metadata.getColumnName(i);
      headers.add(new Header(fieldName, Object.class, false));
    }
    return headers;
  }

  public static Object sqlArrayToList(Class<?> listClass, Array a) {
    try {
      Object[] objectArray = (Object[]) a.getArray();
      return objectArrayToList(listClass, objectArray);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<?> objectArrayToList(Class<?> listClass, Object[] array) {
    return streamToList(listClass, Arrays.stream(array));
  }

  public static List<?> streamToList(Class<?> listClass, Stream<Object> stream) {
    if (listClass == Lists.LongList.class) {
      return stream
              .map(e -> e instanceof BigInteger ? ((BigInteger) e).longValueExact() : ((Number) e).longValue())
              .collect(Collectors.toCollection(Lists.LongList::new));
    } else if (listClass == Lists.DoubleList.class) {
      return stream
              .map(e -> (Double) e)
              .collect(Collectors.toCollection(Lists.DoubleList::new));
    } else if (listClass == Lists.LocalDateList.class) {
      return stream
              // Clickhouse does not return a java.sql.Date but a LocalDate directly but not Spark
              .map(e -> e instanceof LocalDate ? (LocalDate) e : ((Date) e).toLocalDate())
              .collect(Collectors.toCollection(Lists.LocalDateList::new));
    } else {
      return stream.toList();
    }
  }

  public static Class<?> getListClassFromElementClass(Class<?> elementClass) {
    if (elementClass.equals(double.class) || elementClass.equals(float.class)) {
      return Lists.DoubleList.class;
    } else if (elementClass.equals(long.class) || elementClass.equals(int.class)) {
      return Lists.LongList.class;
    } else if (elementClass.equals(LocalDate.class)) {
      return Lists.LocalDateList.class;
    } else if (elementClass.equals(String.class)) {
      return Lists.StringList.class;
    } else {
      return List.class; // we convert Array to List
    }
  }
}
