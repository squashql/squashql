package io.squashql.query.dto;

import io.squashql.query.ColumnSet;
import io.squashql.query.ColumnSetKey;
import io.squashql.query.Field;
import io.squashql.query.TableField;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class BucketColumnSetDto implements ColumnSet {

  public Field name;

  public Field field;

  public Map<String, List<String>> values = new LinkedHashMap<>();

  public BucketColumnSetDto(String name, Field field) {
    this.name = new TableField(name);
    this.field = field;
  }

  public BucketColumnSetDto withNewBucket(String bucketName, List<String> bucketValues) {
    this.values.put(bucketName, new ArrayList<>(bucketValues));
    return this;
  }

  @Override
  public List<Field> getColumnsForPrefetching() {
    return List.of(this.field);
  }

  @Override
  public List<Field> getNewColumns() {
    return List.of(this.name, this.field);
  }

  @Override
  public ColumnSetKey getColumnSetKey() {
    return ColumnSetKey.BUCKET;
  }
}
