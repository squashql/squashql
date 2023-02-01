package io.squashql.query;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.*;
import io.squashql.BigQueryUtil;

public class BigQueryTestUtil {

  public static final String SERVICE_ACCOUNT_KEY_FILE_PATH = System.getProperty("bigquery.test.key.file.path");
  public static final String SERVICE_ACCOUNT_KEY = System.getProperty("bigquery.test.key");
  public static final String PROJECT_ID = System.getProperty("bigquery.test.project.id");
  public static final String DATASET_NAME = System.getProperty("bigquery.test.dataset.name");

  public static final ServiceAccountCredentials createServiceAccountCredentials() {
    if (SERVICE_ACCOUNT_KEY_FILE_PATH != null) {
      return BigQueryUtil.createCredentialsFromFile(SERVICE_ACCOUNT_KEY_FILE_PATH);
    } else if (SERVICE_ACCOUNT_KEY != null) {
      return BigQueryUtil.createCredentialsFromFileContent(SERVICE_ACCOUNT_KEY);
    } else {
      throw new IllegalStateException("no service account credentials provided");
    }
  }

  public static void createDatasetIfDoesNotExist(BigQuery bigquery, String datasetName) {
    DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
    try {
      bigquery.create(datasetInfo);
    } catch (BigQueryException e) {
      if (e.getCode() == 409 && e.getReason().equals("duplicate")) {
        // ignore
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  public static void deleteTable(BigQuery bigQuery, String tableName) {
    TableId tableId = TableId.of(DATASET_NAME, tableName);
    Table table = bigQuery.getTable(tableId);
    boolean deleted = table.delete();
    if (deleted) {
      // the table was deleted
      System.out.println("Table " + tableId + " successfully deleted");
    } else {
      // the table was not found
      System.out.println("Table " + tableId + " could not be deleted");
    }
  }

  /**
   * See {@link io.squashql.BigQueryUtil}.
   */
  public static Object translate(Object o) {
    if (o == null) {
      return null;
    }

    if (o.getClass().equals(int.class) || o.getClass().equals(Integer.class)) {
      return ((Number) o).longValue();
    } else {
      return o;
    }
  }
}
