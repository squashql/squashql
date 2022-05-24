package me.paulbares.query;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import me.paulbares.ClickHouseDatastore;
import me.paulbares.ClickHouseUtil;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ClickHouseQueryEngine extends AQueryEngine<ClickHouseDatastore> {

  public ClickHouseQueryEngine(ClickHouseDatastore datastore) {
    super(datastore);
  }

  @Override
  protected Table retrieveAggregates(QueryDto query) {
    String sql = SQLTranslator.translate(query, this.fieldSupplier);

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
      List<List<Object>> rows = new ArrayList<>();
      response.records().iterator().forEachRemaining(r -> {
        List<Object> a = new ArrayList<>(r.size());
        r.forEach(e -> a.add(e.asObject()));
        rows.add(a);
      });
      return new ArrayTable(fields, rows);
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
