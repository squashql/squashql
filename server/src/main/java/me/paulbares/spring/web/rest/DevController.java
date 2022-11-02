package me.paulbares.spring.web.rest;

import me.paulbares.query.database.QueryEngine;
import me.paulbares.util.TableTSCodeGenerator;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("dev")
public class DevController {

  protected final QueryEngine queryEngine;

  public DevController(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  @GetMapping("/ts")
  public String getTypescriptTableClasses() {
    return TableTSCodeGenerator.getFileContent(this.queryEngine.datastore());
  }
}
