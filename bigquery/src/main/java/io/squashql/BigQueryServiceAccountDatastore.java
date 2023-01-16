package io.squashql;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.common.base.Suppliers;
import io.squashql.store.Field;
import io.squashql.store.Store;

import java.util.*;
import java.util.function.Supplier;

/**
 * Implementation of {@link BigQueryDatastore} that uses a {@link ServiceAccountCredentials}.
 */
public class BigQueryServiceAccountDatastore implements BigQueryDatastore {

  private final Supplier<Map<String, Store>> stores;
  private final BigQuery bigquery;
  private final String projectId;
  private final String datasetName;

  public BigQueryServiceAccountDatastore(ServiceAccountCredentials credentials, String projectId, String datasetName) {
    this.projectId = projectId;
    this.datasetName = datasetName;
    // Initialize client that will be used to send requests. This client only needs to be created
    // once, and can be reused for multiple requests.
    BigQueryOptions build = BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .setCredentials(credentials)
            .build();
    build.setThrowNotFound(true);
    this.bigquery = build
            .getService();
    this.stores = Suppliers.memoize(
            () -> getTableNames(this.bigquery, projectId, datasetName)
                    .stream()
                    .collect(HashMap::new,
                            (map, table) -> map.put(table, new Store(table, getFields(this.bigquery, datasetName, table))),
                            (x, y) -> {
                            }));
  }

  @Override
  public String getProjectId() {
    return this.projectId;
  }

  @Override
  public String getDatasetName() {
    return this.datasetName;
  }

  @Override
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
      tableNames.add(iterator.next().getTableId().getTable());
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
