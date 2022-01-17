package me.paulbares.query;

public interface QueryEngine {

  String GRAND_TOTAL = "Grand Total";
  String TOTAL = "Total";

  Table execute(Query query);
}
