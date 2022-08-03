package me.paulbares.util;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestCustomExplicitOrdering {

  @Test
  void test() {
    List<String> list = new ArrayList<>(List.of("b", "d", "a", "c", "b"));
    Collections.sort(list, new CustomExplicitOrdering(List.of("b", "c")));
    // d and a not in the list should be order in natural order: a first then d
    Assertions.assertThat(list).containsExactly("b", "b", "c", "a", "d");
  }
}
