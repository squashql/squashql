package me.paulbares;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseDriver;
import com.clickhouse.jdbc.ClickHouseStatement;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class Main {

  public static final String JDBC_URL = "jdbc:clickhouse://localhost:" + ClickHouseProtocol.HTTP.getDefaultPort();

  public static void testConnect() throws SQLException {
    String address = "localhost:" + ClickHouseProtocol.HTTP.getDefaultPort();
    ClickHouseDriver driver = new ClickHouseDriver();
    ClickHouseConnection conn = driver.connect("jdbc:clickhouse://" + address, null);
    conn.close();
  }

  public static ClickHouseDataSource newDataSource(Properties properties) throws SQLException {
    return new ClickHouseDataSource(JDBC_URL, properties);
  }

  public static void main(String[] args) throws Exception {
    testConnect();

    String dbName = Main.class.getSimpleName();
    try (ClickHouseConnection conn = newDataSource(null).getConnection();
         ClickHouseStatement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName);
      stmt.execute("drop table if exists test_read_write_date");
      stmt.execute("create table test_read_write_date(id Int32)engine=Memory");

      {
        PreparedStatement prepareStatement = conn.prepareStatement("insert into test_read_write_date values(?)");
        prepareStatement.setInt(1, 1);
        prepareStatement.addBatch();
        prepareStatement.setInt(1, 2);
        prepareStatement.addBatch();
        int[] results = prepareStatement.executeBatch();
        ResultSet rs = conn.createStatement().executeQuery("select * from test_read_write_date order by id");
        System.out.println();
      }

      // only HTTP and gRPC are supported at this point
      ClickHouseProtocol preferredProtocol = ClickHouseProtocol.HTTP;
      // you'll have to parse response manually if use different format
      ClickHouseFormat preferredFormat = ClickHouseFormat.RowBinaryWithNamesAndTypes;

      // connect to localhost, use default port of the preferred protocol
      ClickHouseNode server = ClickHouseNode.builder().port(preferredProtocol).build();

      try (ClickHouseClient client = ClickHouseClient.newInstance(preferredProtocol);
           ClickHouseResponse response = client.connect(server)
                   .format(preferredFormat)
                   .query("select * from numbers(:limit)")
                   .params(1000).execute().get()) {
        // or resp.stream() if you prefer stream API
        for (ClickHouseRecord record : response.records()) {
          int num = record.getValue(0).asInteger();
          String str = record.getValue(0).asString();
        }

        ClickHouseResponseSummary summary = response.getSummary();
        long totalRows = summary.getTotalRowsToRead();
        System.out.println(totalRows);
      }
    }
  }
}
