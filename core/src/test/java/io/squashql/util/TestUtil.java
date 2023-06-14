package io.squashql.util;

import com.google.common.collect.ImmutableList;
import io.squashql.jackson.JacksonUtil;
import io.squashql.query.Header;
import io.squashql.query.RowTable;
import io.squashql.query.Table;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;
import org.assertj.core.util.Throwables;

import java.io.File;
import java.io.IOException;
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
    File file = new File(TestUtil.class.getClassLoader().getResource(path.toString()).getFile());
    try {
      return JacksonUtil.deserialize(FileUtils.readFileToString(file, "UTF-8"), target);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @NoArgsConstructor // for Jackson
  @AllArgsConstructor
  private static class RowTableJson {

    public List<Header> headers;
    public List<List<Object>> rows;
  }
}
