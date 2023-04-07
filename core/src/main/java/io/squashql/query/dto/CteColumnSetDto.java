package io.squashql.query.dto;

import io.squashql.query.ColumnSet;
import io.squashql.query.ColumnSetKey;
import io.squashql.store.Field;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class CteColumnSetDto implements ColumnSet {

  private static final String cteIdentifier = "__temp_table_cte__";
//  private static final String cteIdentifier = "MYTEMPTABLE";

  public String name;

  public String field;

  public Map<String, List<Object>> values = new HashMap<>();

  public CteColumnSetDto(String name, String field) {
    this.name = name;
    this.field = field;
  }

  public CteColumnSetDto withNewBucket(String bucketName, List<Object> range) {
    this.values.put(bucketName, range);
    return this;
  }

  @Override
  public List<String> getColumnsForPrefetching() {
    return List.of(this.name);
  }

  @Override
  public List<Field> getNewColumns() {
    return List.of(new Field(identifier(), this.name, String.class));
  }

  @Override
  public ColumnSetKey getColumnSetKey() {
    return ColumnSetKey.CTE;
  }

  public static String identifier() {
    return cteIdentifier;
  }

  public static String lowerBoundName() {
    return cteIdentifier + "_min";
  }

  public static String upperBounderName() {
    return cteIdentifier + "_max";
  }
}
