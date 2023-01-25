package io.squashql;

import com.google.cloud.bigquery.BigQuery;
import io.squashql.store.Datastore;

public interface BigQueryDatastore extends Datastore {
  BigQuery getBigquery();

  String getProjectId();

  String getDatasetName();
}
