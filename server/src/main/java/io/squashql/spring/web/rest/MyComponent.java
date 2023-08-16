package io.squashql.spring.web.rest;

import io.squashql.query.QueryCache;
import org.springframework.stereotype.Component;

@Component
public class MyComponent {

  public MyComponent(QueryController queryController) {
    QueryCache queryCache = queryController.queryExecutor.queryCache;
    // TODO
  }
}
