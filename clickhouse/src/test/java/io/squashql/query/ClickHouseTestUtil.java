package io.squashql.query;

import com.clickhouse.client.ClickHouseProtocol;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.function.Function;

import static java.time.temporal.ChronoUnit.SECONDS;

public class ClickHouseTestUtil {

  protected static final boolean printAll = true;

  protected static final Function<GenericContainer, String> jdbcUrl = c -> String.format("jdbc:ch:http://%s:%d",
          c.getHost(),
          c.getMappedPort(ClickHouseProtocol.HTTP.getDefaultPort()));

  protected static GenericContainer createClickHouseContainer() {
    return new GenericContainer(DockerImageName.parse("clickhouse/clickhouse-server:latest"))
            .withExposedPorts(ClickHouseProtocol.HTTP.getDefaultPort(), ClickHouseProtocol.GRPC.getDefaultPort())
            .waitingFor(Wait.forHttp("/ping")
                    .forPort(ClickHouseProtocol.HTTP.getDefaultPort())
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.of(60, SECONDS)))
            .withReuse(true)
            .withLogConsumer(of -> {
              String s = ((OutputFrame) of).getUtf8String();
              if (printAll) {
                System.out.print("ClickHouseContainer Container >>>" + s);
              }
            });
  }
}
