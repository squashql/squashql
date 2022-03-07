package me.paulbares.query;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import me.paulbares.ClickHouseDatastore;
import me.paulbares.ClickHouseStore;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.ConditionType;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.SingleValueConditionDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static me.paulbares.query.QueryBuilder.eq;
import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

public class ClickHouseQueryEngine extends AQueryEngine {

  public ClickHouseQueryEngine(ClickHouseDatastore datastore) {
    super(datastore);
  }

  @Override
  protected Table retrieveAggregates(QueryDto query) {
    addScenarioConditionIfNecessary(query);
    replaceScenarioFieldName(query);
    String sql = SQLTranslator.translate(query, this.fieldSupplier);

    // only HTTP and gRPC are supported at this point
    ClickHouseProtocol preferredProtocol = ClickHouseProtocol.HTTP;
    // you'll have to parse response manually if use different format
    ClickHouseFormat preferredFormat = ClickHouseFormat.RowBinaryWithNamesAndTypes;

    // connect to localhost, use default port of the preferred protocol
    ClickHouseNode server = ClickHouseNode.builder()
            .host(((ClickHouseDatastore) this.datastore).dataSource.getHost())
            .port(((ClickHouseDatastore) this.datastore).dataSource.getPort())
            .build();

    String scenarioFieldName = ((ClickHouseDatastore) this.datastore).stores.get(query.table.name).scenarioFieldName();
    try (ClickHouseClient client = ClickHouseClient.newInstance(preferredProtocol);
         ClickHouseResponse response = client.connect(server)
                 .format(preferredFormat)
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
                  return new Field(c.getColumnName(), ClickHouseStore.clickHouseTypeToClass(c.getDataType()));
                }
              })
              .toList();
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

  private void replaceScenarioFieldName(QueryDto query) {
    String key = ((ClickHouseDatastore) this.datastore).stores.get(query.table.name).scenarioFieldName();
    ConditionDto cond = query.conditions.remove(SCENARIO_FIELD_NAME);
    if (cond != null) {
      query.conditions.put(key, cond);
    }

    // Order here is important.
    if (query.coordinates.containsKey(SCENARIO_FIELD_NAME)) {
      List<String> coord = query.coordinates.get(SCENARIO_FIELD_NAME);
      Map<String, List<String>> newCoords = new LinkedHashMap<>();
      for (Map.Entry<String, List<String>> entry : query.coordinates.entrySet()) {
        if (entry.getKey().equals(SCENARIO_FIELD_NAME)) {
          newCoords.put(key, coord);
        } else {
          newCoords.put(entry.getKey(), entry.getValue());
        }
      }
      query.coordinates = newCoords;
    }
  }

  protected void addScenarioConditionIfNecessary(QueryDto query) {
    if (!query.coordinates.containsKey(SCENARIO_FIELD_NAME)) {
      ConditionDto c = query.conditions.get(SCENARIO_FIELD_NAME);
      if (c == null) {
        // If no condition, default to base by adding a condition and let Spark handle it :)
        query.condition(SCENARIO_FIELD_NAME, eq(Datastore.MAIN_SCENARIO_NAME));
      } else {
        // Only support single value condition. Otherwise, it does not make sense.
        if (!(c instanceof SingleValueConditionDto s
                && (s.type == ConditionType.EQ || (s.type == ConditionType.IN && ((Set<Object>) s.value).size() == 1)))) {
          String format = String.format("""
                  Query %s is not correct. Field s% should be in the coordinates or if not in a
                  single value condition.
                  """, query, SCENARIO_FIELD_NAME);
          throw new IllegalArgumentException(format);
        }
      }
    }
  }
}
