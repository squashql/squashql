package io.squashql.query.dto;

import io.squashql.query.Measure;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class MetadataResultDto {

  public List<StoreMetadata> stores;
  public List<String> aggregationFunctions;
  public List<Measure> measures;

  @NoArgsConstructor
  @AllArgsConstructor
  @EqualsAndHashCode
  @ToString
  public static class StoreMetadata {

    public String name;
    public List<MetadataItem> fields;
  }
}
