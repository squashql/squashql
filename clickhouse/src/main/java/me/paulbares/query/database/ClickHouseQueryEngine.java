package me.paulbares.query.database;

import com.clickhouse.client.*;
import me.paulbares.ClickHouseDatastore;
import me.paulbares.ClickHouseUtil;
import me.paulbares.query.ColumnarTable;
import me.paulbares.query.Table;
import me.paulbares.store.Field;
import org.eclipse.collections.api.tuple.Pair;

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

    try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
         ClickHouseResponse response = client.connect(server)
                 .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                 .query(sql)
                 .execute()
                 .get()) {
      Pair<List<Field>, List<List<Object>>> result = transform(response.getColumns(),
              c -> new Field(c.getColumnName(), ClickHouseUtil.clickHouseTypeToClass(c.getDataType())),
              response.records().iterator(),
              (i, r) -> r.getValue(i).asObject());
      return new ColumnarTable(
              result.getOne(),
              query.measures,
              IntStream.range(query.coordinates.size(), query.coordinates.size() + query.measures.size()).toArray(),
              IntStream.range(0, query.coordinates.size()).toArray(),
              result.getTwo());
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}