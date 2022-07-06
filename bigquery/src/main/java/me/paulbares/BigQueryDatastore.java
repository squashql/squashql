package me.paulbares;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.common.base.Suppliers;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.store.Store;

import java.util.*;
import java.util.function.Supplier;

public class BigQueryDatastore implements Datastore {

  private final ServiceAccountCredentials credentials;
  private final BigQuery bigquery;
  public final Supplier<Map<String, Store>> stores;
  public final String projectId;
  public final String datasetName;

  public BigQueryDatastore(ServiceAccountCredentials credentials, String projectId, String datasetName) {
    this.credentials = credentials;
    this.projectId = projectId;
    this.datasetName = datasetName;
    // Initialize client that will be used to send requests. This client only needs to be created
    // once, and can be reused for multiple requests.
    this.bigquery = BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .setCredentials(credentials)
            .build()
            .getService();

    this.stores = Suppliers.memoize(
            () -> getTableNames(this.bigquery, projectId, datasetName)
                    .stream()
                    .collect(() -> new HashMap<>(),
                            (map, table) -> map.put(table, new Store(table, getFields(this.bigquery, datasetName, table))),
                            (x, y) -> {
                            }));
  }

  public BigQuery getBigquery() {
    return this.bigquery;
  }

  @Override
  public Map<String, Store> storesByName() {
    return this.stores.get();
  }

  public static Collection<String> getTableNames(BigQuery query, String projectId, String datasetName) {
    Page<Table> tablePage = query.listTables(datasetName);
    Set<String> tableNames = new HashSet<>();
    Iterator<Table> iterator = tablePage.getValues().iterator();
    while (iterator.hasNext()) {
      String table = iterator.next().getTableId().getTable();
//      tableNames.add(projectId + "." + datasetName + "." + table);
      tableNames.add(table);
    }

    return tableNames;
  }

  public static List<Field> getFields(BigQuery query, String datasetName, String tableName) {
    List<Field> fields = new ArrayList<>();
    Schema schema = query.getTable(datasetName, tableName).getDefinition().getSchema();
    for (com.google.cloud.bigquery.Field field : schema.getFields()) {
      fields.add(new Field(field.getName(), BigQueryUtil.bigQueryTypeToClass(field.getType())));
    }
    return fields;
  }
}
