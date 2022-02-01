package me.paulbares.spring.web.rest;

import me.paulbares.client.HttpClientQuerier;
import me.paulbares.client.SimpleTable;
import me.paulbares.query.context.Totals;
import me.paulbares.query.dto.QueryDto;
import org.apache.catalina.webresources.TomcatURLStreamHandlerFactory;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;

import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class HttpClientQuerierTest {

  static {
    // FIXME: why do I need to do this? (fails in maven build without it)
    // Fix found here https://github.com/spring-projects/spring-boot/issues/21535
    TomcatURLStreamHandlerFactory.disable();
  }

  @LocalServerPort
  int port;

  @Test
  void testGetMetadata() {
    String url = "http://127.0.0.1:" + this.port;

    var querier = new HttpClientQuerier(url);
    SparkQueryControllerTest.assertMetadataResult(querier.metadata());
  }

  @Test
  void testRunQuery() {
    String url = "http://127.0.0.1:" + this.port;

    var querier = new HttpClientQuerier(url);

    QueryDto query = new QueryDto()
            .table("products")
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .context(Totals.KEY, Totals.VISIBLE_TOP)
            .aggregatedMeasure("marge", "sum");

    SimpleTable table = querier.run(query);
    SparkQueryControllerTest.assertQueryWithTotals(table);
  }
}
