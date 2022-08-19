package me.paulbares.js;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;

public class TestJavascriptLibrary {

  @Test
  void testReadJson() {
    URL url = this.getClass().getResource("../../");
    File myObj = new File("filename.txt");
  }
}
