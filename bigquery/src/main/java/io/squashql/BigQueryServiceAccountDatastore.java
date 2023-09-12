package io.squashql;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.base.Suppliers;
import io.squashql.store.Store;
import io.squashql.type.TableField;
import io.squashql.type.TypedField;

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
    this.stores = Suppliers.memoize(() -> fetchStoresByName(this));
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

  public static Map<String, Store> fetchStoresByName(BigQueryDatastore datastore) {
    BigQuery bigquery = datastore.getBigquery();
    String datasetName = datastore.getDatasetName();
    return getTableNames(bigquery, datastore.getProjectId(), datasetName)
            .stream()
            .collect(HashMap::new,
                    (map, table) -> {
                      List<TypedField> fields = getFieldsOrNull(bigquery, datasetName, table);
                      if (fields != null) {
                        map.put(table, new Store(table, fields));
                      }
                    },
                    (x, y) -> {
                    });
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

  public static List<TypedField> getFieldsOrNull(BigQuery query, String datasetName, String tableName) {
    List<TypedField> fields = new ArrayList<>();
    try {
      Schema schema = query.getTable(datasetName, tableName).getDefinition().getSchema();
      for (com.google.cloud.bigquery.Field field : schema.getFields()) {
        fields.add(new TableField(tableName, field.getName(), BigQueryUtil.bigQueryTypeToClass(field.getType())));
      }
      return fields;
    } catch (Exception e) {
      if (e instanceof BigQueryException bqe && bqe.getCode() == 403) {
        // Ignore, the user is not allowed to see this table
        return null;
      }
      throw e;
    }
  }
}
