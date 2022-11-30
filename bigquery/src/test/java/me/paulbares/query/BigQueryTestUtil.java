package me.paulbares.query;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetInfo;

public class BigQueryTestUtil {

  public static final String CREDENTIALS = "/Users/paul/dev/unittests-370209-0eb082b518e1.json";
  public static final String PROJECT_ID = "unittests-370209";

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
}
