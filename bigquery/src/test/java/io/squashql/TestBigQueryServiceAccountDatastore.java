package io.squashql;

import com.google.cloud.PageImpl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestBigQueryServiceAccountDatastore {

  private static final String DATASET = "dataset";

  @Test
  void testGetTableNamesReadsEveryPage() {
    PageImpl<Table> secondPage = new PageImpl<>(() -> null, null, List.of(table("c")));
    PageImpl<Table> firstPage = new PageImpl<>(() -> secondPage, "next-page-token", List.of(table("a"), table("b")));
    BigQuery bigquery = mock(BigQuery.class);
    when(bigquery.listTables(DATASET)).thenReturn(firstPage);

    assertThat(BigQueryServiceAccountDatastore.getTableNames(bigquery, "project", DATASET))
            .containsExactlyInAnyOrder("a", "b", "c");
  }

  private static Table table(String name) {
    Table table = mock(Table.class);
    when(table.getTableId()).thenReturn(TableId.of(DATASET, name));
    return table;
  }
}
