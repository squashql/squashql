package me.paulbares.query;

import me.paulbares.query.dto.QueryDto;

public interface QueryEngine {

  String GRAND_TOTAL = "Grand Total";
  String TOTAL = "Total";

  Table execute(QueryDto query);
}
