package me.paulbares.spring.dataset;

import me.paulbares.BigQueryDatastore;
import me.paulbares.BigQueryUtil;
import me.paulbares.query.database.BigQueryEngine;
import me.paulbares.query.database.QueryEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

/**
 * JVM parameters: -Dspring.profiles.active=optiprix -Dbigquery.credentials.path=/Users/paul/dev/aitmindiceprix-686299293f2f.json
 */
@Configuration
@Profile({"optiprix", "cdg"})
public class OptiprixOrCDGDatasetConfig {

  @Autowired
  private Environment env;

  @Bean
  @ConditionalOnMissingBean // This is for the tests. Tests can use another one if defined.
  public QueryEngine queryEngine() {
    String credendialsPath = this.env.getProperty("bigquery.credentials.path");
    String datasetName = this.env.getProperty("datasetName");
    String projectId = this.env.getProperty("projectId");
    BigQueryDatastore datastore = new BigQueryDatastore(BigQueryUtil.createCredentials(credendialsPath), projectId, datasetName);
    return new BigQueryEngine(datastore);
  }
}
