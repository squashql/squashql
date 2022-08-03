package me.paulbares.client;

import java.util.List;
import java.util.Map;

public class SimpleTableWithMetadata {

  public Map<String, Object> table;
  public List<Map<String, String>> metadata;

//  {"table":"{\"columns\":[\"scenario\",\"sum(quantity)\"],\"rows\":[[\"MDD up\",4000],[\"MN & MDD down\",4000],[\"MN & MDD up\",4000],[\"MN up\",4000],[\"base\",4000]]}","metadata":[{"name":"scenario","type":"string"},{"expression":"sum(quantity)","name":"sum(quantity)","type":"long"}]}
}
