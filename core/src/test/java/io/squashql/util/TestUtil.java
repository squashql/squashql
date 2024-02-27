package io.squashql.util;

import com.google.common.collect.ImmutableList;
import io.squashql.jackson.JacksonUtil;
import io.squashql.query.*;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.dto.CacheStatsDto;
import io.squashql.query.dto.SimpleTableDto;
import io.squashql.table.ColumnarTable;
import io.squashql.table.RowTable;
import io.squashql.table.Table;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;
import org.assertj.core.util.Throwables;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TestUtil {

  public static void assertCacheStats(GlobalCache cache, int hitCount, int missCount) {
    CacheStatsDto stats = cache.stats(null);
    assertCacheStats(stats, hitCount, missCount);
  }

  public static void assertCacheStats(GlobalCache cache, int hitCount, int missCount, SquashQLUser user) {
    CacheStatsDto stats = cache.stats(user);
    assertCacheStats(stats, hitCount, missCount);
  }

  public static void assertCacheStats(CacheStatsDto stats, int hitCount, int missCount) {
    Assertions.assertThat(stats.hitCount).isEqualTo(hitCount);
    Assertions.assertThat(stats.missCount).isEqualTo(missCount);
  }

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
   * To format the json: https://jsonformatter.curiousconcept.com/#
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

  public static String readAllLines(String fileName) {
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(Paths.get(fileName).toString());
         BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
      return readAllLines(reader);
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

  public static ColumnarTable convert(RowTable rowTable, Set<CompiledMeasure> measures) {
    List<List<Object>> values = new ArrayList<>(rowTable.headers().size());
    for (int i = 0; i < rowTable.headers().size(); i++) {
      values.add(new ArrayList<>());
    }
    rowTable.forEach(row -> {
      for (int h = 0; h < row.size(); h++) {
        values.get(h).add(row.get(h));
      }
    });
    List<Header> headers = new ArrayList<>();
    Set<String> measureNames = measures.stream().map(CompiledMeasure::alias).collect(Collectors.toSet());
    for (Header header : rowTable.headers()) {
      if (measureNames.contains(header.name())) {
        headers.add(new Header(header.name(), header.type(), true));
      } else {
        headers.add(header);
      }
    }
    return new ColumnarTable(headers, measures, values);
  }

  /**
   * Do not work if the query contains totals or sub-totals.
   */
  public static SimpleTableDto cellsToTable(List<Map<String, Object>> cells, List<String> columns) {
    List<List<Object>> rows = new ArrayList<>(columns.size());
    for (Map<String, Object> cell : cells) {
      List<Object> row = new ArrayList<>(columns.size());
      for (String column : columns) {
        row.add(cell.get(column));
      }
      rows.add(row);
    }
    return new SimpleTableDto(columns, rows);
  }

  @NoArgsConstructor // for Jackson
  @AllArgsConstructor
  private static class RowTableJson {

    public List<Header> headers;
    public List<List<Object>> rows;
  }
}
