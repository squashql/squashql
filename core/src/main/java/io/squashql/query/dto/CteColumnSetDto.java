package io.squashql.query.dto;

import io.squashql.query.ColumnSet;
import io.squashql.query.ColumnSetKey;
import io.squashql.store.Field;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.eclipse.collections.api.tuple.Pair;

import java.util.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class CteColumnSetDto implements ColumnSet {

  private static final String cteIdentifier = "__temp_table_cte__";
//  private static final String cteIdentifier = "MYTEMPTABLE";

  public String name;

  public String field;

  public Map<String, Pair<Object, Object>> values = new HashMap<>();

  public CteColumnSetDto(String name, String field) {
    this.name = name;
    this.field = field;
  }

  public CteColumnSetDto withNewBucket(String bucketName, Pair<Object, Object> range) {
    this.values.put(bucketName, range);
    return this;
  }

  @Override
  public List<String> getColumnsForPrefetching() {
    return List.of();
  }

  @Override
  public List<Field> getNewColumns() {
    return List.of(new Field(null, this.name, String.class));
  }

  @Override
  public ColumnSetKey getColumnSetKey() {
    return ColumnSetKey.CTE;
  }

  public String generateExpression() {
    StringBuilder sb = new StringBuilder();
    Iterator<Map.Entry<String, Pair<Object, Object>>> it = this.values.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, Pair<Object, Object>> entry = it.next();
      sb.append("select");
      sb.append(" '").append(entry.getKey()).append("' as " + this.name + ", ");
      sb.append(" ").append(entry.getValue().getOne()).append(" as " + lowerBoundName() + ", ");
      sb.append(" ").append(entry.getValue().getTwo()).append(" as " + upperBounderName());
      if (it.hasNext()) {
        sb.append(" union all ");
      }
    }

    return sb.toString();
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
