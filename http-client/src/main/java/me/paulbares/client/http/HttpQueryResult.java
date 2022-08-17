package me.paulbares.client.http;

import java.util.List;
import java.util.Map;

public class HttpQueryResult {

  public SimpleTable table;
  public List<Map<String, String>> metadata;
  public DebugDto debug;
}
