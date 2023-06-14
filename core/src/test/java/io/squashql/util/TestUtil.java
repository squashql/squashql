package io.squashql.util;

import com.google.common.collect.ImmutableList;
import io.squashql.jackson.JacksonUtil;
import io.squashql.query.Header;
import io.squashql.query.RowTable;
import io.squashql.query.Table;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;
import org.assertj.core.util.Throwables;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class TestUtil {

  public static ThrowableAssert<Throwable> assertThatThrownBy(ThrowableAssert.ThrowingCallable shouldRaiseThrowable) {
    try {
      shouldRaiseThrowable.call();
      Assertions.fail("should have thrown an exception");
      return null;
    } catch (Throwable t) {
      Throwable rootCause = t.getCause() == null ? t : Throwables.getRootCause(t);
      return new ThrowableAssert<>(rootCause);
    }
  }

  /**
   * To format the json: https://jsonlint.com/
   */
  public static String tableToJson(Table table) {
    RowTable rowTable = new RowTable(table.headers(), ImmutableList.copyOf(table.iterator()));
    return JacksonUtil.serialize(Map.of("headers", rowTable.headers(), "rows", rowTable));
  }

  public static Table deserializeTableFromFile(Path path) {
    RowTableJson rowTable = deserializeFromFile(path, RowTableJson.class);
    return new RowTable(rowTable.headers, rowTable.rows);
  }

  public static <T> T deserializeFromFile(Path path, Class<T> target) {
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(path.toString());
         BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
      return JacksonUtil.deserialize(readAllLines(reader), target);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String readAllLines(BufferedReader reader) throws IOException {
    StringBuilder content = new StringBuilder();
    String line;

    while ((line = reader.readLine()) != null) {
      content.append(line);
      content.append(System.lineSeparator());
    }

    return content.toString();
  }

  @NoArgsConstructor // for Jackson
  @AllArgsConstructor
  private static class RowTableJson {

    public List<Header> headers;
    public List<List<Object>> rows;
  }
}
