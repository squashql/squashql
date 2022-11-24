package me.paulbares.transaction;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface TransactionManager {

  String MAIN_SCENARIO_NAME = "base";

  String SCENARIO_FIELD_NAME = "scenario";

  void load(String scenario, String store, List<Object[]> tuples);

  void loadCsv(String scenario, String store, String path, String delimiter, boolean header);

  static String scenarioStoreName(String store, String scenario) {
    if (scenario.equals(MAIN_SCENARIO_NAME)) {
      return store;
    } else {
      return "__" + store + "_scenario_" + scenario + "__";
    }
  }

  static String extractScenarioFromStoreName(String baseStoreName, String scenarioStoreName) {
    Pattern compile = Pattern.compile("__" + baseStoreName + "_scenario_(.*)__");
    Matcher matcher = compile.matcher(scenarioStoreName);
    if (matcher.find()) {
      return matcher.group(1);
    } else {
      return null;
    }
  }
}
