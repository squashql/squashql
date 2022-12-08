package me.paulbares.query.database;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import me.paulbares.SnowflakeDatastore;
import me.paulbares.SnowflakeUtil;
import me.paulbares.query.ColumnarTable;
import me.paulbares.query.Measure;
import me.paulbares.query.Table;
import me.paulbares.store.Field;

import java.util.List;
import java.util.stream.IntStream;

public class SnowflakeEngine extends AQueryEngine<SnowflakeDatastore> {

  private final QueryRewriter queryRewriter;

  public SnowflakeEngine(SnowflakeDatastore datastore) {
    super(datastore);
    this.queryRewriter = new SnowflakeQueryRewriter();
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query) {
    String sql = SQLTranslator.translate(query, this.fieldSupplier, queryRewriter, QueryRewriter::tableName);
    System.out.println(sql);
    try {
      ResultSet tableResult = this.datastore.getSnowflake().executeQuery(sql);

      List<Field> headers = new ArrayList<>();
      ResultSetMetaData metadata = tableResult.getMetaData();
      // get the column names; column indexes start from 1
      for (int i = 1; i < metadata.getColumnCount() + 1; i++) {
        headers.add(new Field(metadata.getColumnName(i), SnowflakeUtil.sqlTypeToClass(metadata.getColumnType(i))));
      }
      List<List<Object>> values = new ArrayList<>();
      headers.forEach(field -> values.add(new ArrayList<>()));
      while (tableResult.next()) {
        for (int i = 0; i < headers.size(); i++) {
          values.get(i).add(tableResult.getObject(1+i));
        }
      }
      return new ColumnarTable(
              headers,
              query.measures,
              IntStream.range(query.select.size(), query.select.size() + query.measures.size()).toArray(),
              IntStream.range(0, query.select.size()).toArray(),
              values);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    // TODO : to define
    return null;
  }

  static class SnowflakeQueryRewriter implements QueryRewriter {

    @Override
    public String fieldName(String field) {
      return SqlUtils.doubleQuoteEscape(field);
    }

    @Override
    public String measureAlias(String alias) {
      return SqlUtils.doubleQuoteEscape(alias);
    }

    @Override
    public boolean doesSupportPartialRollup() {
      return true;
    }

  }

}
