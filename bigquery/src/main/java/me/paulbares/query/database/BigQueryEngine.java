package me.paulbares.query.database;

import com.google.cloud.bigquery.*;
import me.paulbares.BigQueryDatastore;
import me.paulbares.BigQueryUtil;
import me.paulbares.query.ColumnarTable;
import me.paulbares.query.Measure;
import me.paulbares.query.Table;
import me.paulbares.store.Field;
import org.eclipse.collections.api.tuple.Pair;

import java.util.List;
import java.util.stream.IntStream;

public class BigQueryEngine extends AQueryEngine<BigQueryDatastore> {

  public BigQueryEngine(BigQueryDatastore datastore) {
    super(datastore);
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query) {
    String sql = SQLTranslator.translate(query, null, this.fieldSupplier, new QueryRewriter() {
      @Override
      public String tableName(String table) {
        return SqlUtils.escape(datastore.projectId + "." + datastore.datasetName + "." + table);
      }

      @Override
      public String measureAlias(String alias, Measure measure) {
        String a = alias.replace("(", "_");
        a = a.replace(")", "_");
        return a;
      }
    });

    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
    try {
      TableResult tableResult = this.datastore.getBigquery().query(queryConfig);
      Schema schema = tableResult.getSchema();
      Pair<List<Field>, List<List<Object>>> result = AQueryEngine.transform(
              schema.getFields(),
              f -> new Field(f.getName(), BigQueryUtil.bigQueryTypeToClass(f.getType())),
              tableResult.iterateAll().iterator(),
              (i, fieldValueList) -> read(fieldValueList, schema, i)
      );
      return new ColumnarTable(
              result.getOne(),
              query.measures,
              IntStream.range(query.coordinates.size(), query.coordinates.size() + query.measures.size()).toArray(),
              IntStream.range(0, query.coordinates.size()).toArray(),
              result.getTwo());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private Object read(FieldValueList fieldValues, Schema schema, int index) {
    FieldValue fieldValue = fieldValues.get(index);
    com.google.cloud.bigquery.Field field = schema.getFields().get(index);
    return switch (field.getType().getStandardType()) {
      case BOOL -> fieldValue.getBooleanValue();
      case INT64 -> fieldValue.getLongValue();
      case FLOAT64 -> fieldValue.getDoubleValue();
      case BYTES -> fieldValue.getBytesValue();
      default -> fieldValue.getValue();
    };
  }
}