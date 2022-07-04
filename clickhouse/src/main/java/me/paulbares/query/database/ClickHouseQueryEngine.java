package me.paulbares.query.database;

import com.clickhouse.client.*;
import me.paulbares.ClickHouseDatastore;
import me.paulbares.ClickHouseUtil;
import me.paulbares.query.ColumnarTable;
import me.paulbares.query.Table;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class ClickHouseQueryEngine extends AQueryEngine<ClickHouseDatastore> {

  public ClickHouseQueryEngine(ClickHouseDatastore datastore) {
    super(datastore);
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query) {
    String sql = SQLTranslator.translate(query, null, this.fieldSupplier);

    // connect to localhost, use default port of the preferred protocol
    ClickHouseNode server = ClickHouseNode.builder()
            .host(this.datastore.dataSource.getHost())
            .port(this.datastore.dataSource.getPort())
            .build();

    String scenarioFieldName = this.datastore.storesByName()
            .get(query.table.name)
            .scenarioFieldName();
    try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
         ClickHouseResponse response = client.connect(server)
                 .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                 .query(sql)
                 .execute()
                 .get()) {
      List<Field> fields = response
              .getColumns()
              .stream()
              .map(c -> {
                if (c.getColumnName().equals(scenarioFieldName)) {
                  return new Field(Datastore.SCENARIO_FIELD_NAME, String.class);
                } else {
                  return new Field(c.getColumnName(), ClickHouseUtil.clickHouseTypeToClass(c.getDataType()));
                }
              })
              .toList();
      // Read the records here to close the underlying input stream (response)
      List<List<Object>> values = new ArrayList<>();
      for (int i = 0; i < fields.size(); i++) {
        values.add(new ArrayList<>());
      }
      response.records().iterator().forEachRemaining(r -> {
        for (int i = 0; i < r.size(); i++) {
          values.get(i).add(r.getValue(i).asObject());
        }
      });
      return new ColumnarTable(
              fields,
              query.measures,
              IntStream.range(query.coordinates.size(), query.coordinates.size() + query.measures.size()).toArray(),
              IntStream.range(0, query.coordinates.size()).toArray(),
              values);
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
