package me.paulbares.js;

import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.dto.QueryDto;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class TestJavascriptLibrary {

  @Test
  void testReadJson() throws IOException {
    String name = "query.json";
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(name).getFile());
    String data = FileUtils.readFileToString(file, "UTF-8");
    JacksonUtil.deserialize(data, QueryDto.class);// we do not test anything yet, simply that it does not throw.
  }
}
