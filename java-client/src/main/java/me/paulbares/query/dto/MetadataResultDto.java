package me.paulbares.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import me.paulbares.query.Measure;

import java.util.List;

@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class MetadataResultDto {

  public List<StoreMetadata> stores;
  public List<String> aggregationFunctions;
  public List<Measure> measures;

  public MetadataResultDto(List<StoreMetadata> stores, List<String> aggregationFunctions, List<Measure> measures) {
    this.stores = stores;
    this.aggregationFunctions = aggregationFunctions;
    this.measures = measures;
  }

  @NoArgsConstructor
  @EqualsAndHashCode
  @ToString
  public static class StoreMetadata {

    public String name;
    public List<MetadataItem> fields;

    public StoreMetadata(String name, List<MetadataItem> fields) {
      this.name = name;
      this.fields = fields;
    }
  }
}
