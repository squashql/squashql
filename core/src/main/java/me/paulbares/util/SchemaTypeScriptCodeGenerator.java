package me.paulbares.util;

import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.store.Store;

import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaTypeScriptCodeGenerator {

  public static String transformName(String fieldName, boolean shouldStartWithUpperCase) {
    Pattern compile = Pattern.compile("[^A-Za-z0-9]");
    Matcher matcher = compile.matcher(fieldName);
    int start, end = -1;
    StringBuilder sb = new StringBuilder();
    while (matcher.find()) {
      start = end + 1;
      end = matcher.start();
      if (start != end) {
        sb.append(fieldName.substring(start, start + 1).toUpperCase());
        sb.append(fieldName, start + 1, end);
      } else {
        sb.append(fieldName, start, end);
      }
    }

    start = end + 1;
    end = fieldName.length();

    if (start != end) {
      sb.append(fieldName.substring(start, start + 1).toUpperCase());
      sb.append(fieldName, start + 1, end);
    } else {
      sb.append(fieldName, start, end);
    }

    return (shouldStartWithUpperCase ? sb.substring(0, 1).toUpperCase() : sb.substring(0, 1).toLowerCase()) + sb.substring(1);
  }

  public static String getFileContent(Datastore datastore) {
    StringBuilder sb = new StringBuilder();
    Map<String, Store> storesByName = new TreeMap<>(datastore.storesByName()); // order by store name
    storesByName.forEach((storeName, store) -> {
      sb.append("export class ").append(transformName(storeName, true)).append("Table {").append(System.lineSeparator());
      addAttribute(sb, "tableName", storeName);
      for (Field field : store.fields()) {
        addAttribute(sb, transformName(field.name(), false), field.name());
      }
      sb.append('}').append(System.lineSeparator());
    });
    return sb.toString();
  }

  private static void addAttribute(StringBuilder sb, String attributeName, String attributeValue) {
    sb.append("\t readonly ")
            .append(attributeName)
            .append(": string = ")
            .append('"')
            .append(attributeValue)
            .append('"')
            .append(System.lineSeparator());
  }
}
