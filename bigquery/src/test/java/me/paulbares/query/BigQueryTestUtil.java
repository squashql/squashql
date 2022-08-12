package me.paulbares.query;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetInfo;
import me.paulbares.BigQueryDatastore;

public class BigQueryTestUtil {

  public static final String CREDENTIALS = "/Users/paul/dev/canvas-landing-355413-eb118aab8b19.json";
  public static final String PROJECT_ID = "canvas-landing-355413";

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
