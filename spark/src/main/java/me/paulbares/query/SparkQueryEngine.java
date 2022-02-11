package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.SparkStore;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.ConditionType;
import me.paulbares.query.dto.JoinDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.SingleValueConditionDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.store.Store;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.logging.Logger;

import static me.paulbares.query.QueryBuilder.eq;
import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

public class SparkQueryEngine extends AQueryEngine {

  private static final Logger LOGGER = Logger.getLogger(SparkQueryEngine.class.getName());

  public final SparkDatastore datastore;

  public final Function<String, Field> fieldSupplier;

  public SparkQueryEngine(SparkDatastore datastore) {
    this.datastore = datastore;
    this.fieldSupplier = fieldName -> {
      for (Store store : this.datastore.stores.values()) {
        for (Field field : store.getFields()) {
          if (field.name().equals(fieldName)) {
            return field;
          }
        }
      }
      throw new IllegalArgumentException("Cannot find field with name " + fieldName);
    };
  }

  @Override
  protected Table retrieveAggregates(QueryDto query) {
    LOGGER.fine("Executing " + query);
    addScenarioConditionIfNecessary(query);
    replaceScenarioFieldName(query);
    String sql = SQLTranslator.translate(query, this.fieldSupplier);
    LOGGER.info("Translated query #" + query + " to " + sql);
    createOrReplaceTempView(query.table);
    Dataset<Row> ds = this.datastore.spark.sql(sql);
    return new DatasetTable(ds, scenarioFieldName(query));
  }

  private void replaceScenarioFieldName(QueryDto query) {
    String key = scenarioFieldName(query);
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

  protected void createOrReplaceTempView(TableDto table) {
    this.datastore.get(table.name).createOrReplaceTempView(table.name);
    for (JoinDto join : table.joins) {
      this.datastore.get(join.table.name).createOrReplaceTempView(join.table.name);
      createOrReplaceTempView(join.table);
    }
  }

  protected String scenarioFieldName(QueryDto query) {
    return SparkStore.scenarioFieldName(query.table.name);
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
