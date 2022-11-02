package me.paulbares.util;

import me.paulbares.store.Datastore;
import me.paulbares.store.Field;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TableTSCodeGenerator {

  public static String transformName(String fieldName, boolean shouldStartWithUpperCase) {
    Pattern compile = Pattern.compile("[^A-Za-z0-9]");
    Matcher matcher = compile.matcher(fieldName);
    int start, end = -1;
    StringBuilder sb = new StringBuilder();
    while (matcher.find()) {
      start = end + 1;
      end = matcher.start();
      if (start != end && (start > 0 || shouldStartWithUpperCase)) {
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
    }
    return sb.toString();
  }

  public static String fileContent(Datastore datastore) {
    StringBuilder sb = new StringBuilder();
    datastore.storesByName().forEach((name, store) -> {
      sb.append("export class ").append(transformName(name, true)).append("Table {").append(System.lineSeparator());
      for (Field field : store.fields()) {
        sb.append("\t readonly ")
                .append(transformName(field.name(), false))
                .append(": string = ")
                .append('"')
                .append(field.name())
                .append('"')
                .append(System.lineSeparator());
      }
      sb.append('}').append(System.lineSeparator());
    });
    return sb.toString();
  }

  public static void main(String[] args) {
    System.out.println(transformName("hello.world_zob", false));
    System.out.println(transformName("hello.world_zob", false));
    System.out.println(transformName("helloWorldZob", false));
    System.out.println(transformName(".hello.WorldZob", false));
    System.out.println(transformName(".hello.WorldZob", true));
    System.out.println(transformName("helloWorldZob.", false));
    System.out.println(transformName("helloWorldZob.", true));
  }
}
