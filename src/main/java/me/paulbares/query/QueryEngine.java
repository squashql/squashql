package me.paulbares.query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface QueryEngine {

  Dataset<Row> execute(Query query); // FIXME should return spark obj
}
