package me.paulbares.query;

import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.*;

public class BigQueryTestUtil {

  public static final String CREDENTIALS = "";
  public static final String PROJECT_ID = "";
  public static final String DATASET_NAME = "";

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
   * See {@link me.paulbares.BigQueryUtil}.
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
