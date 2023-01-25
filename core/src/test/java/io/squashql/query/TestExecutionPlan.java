package io.squashql.query;

import com.google.common.graph.Graph;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

public class TestExecutionPlan {

  static class Node {
    final Set<Node> children;
    final String name;

    Node(Set<Node> children, String name) {
      this.children = children;
      this.name = name;
    }

    Node(String name) {
      this(Collections.emptySet(), name);
    }

    @Override
    public String toString() {
      return this.name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Node node = (Node) o;
      return Objects.equals(this.name, node.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.name);
    }
  }

  @Test
  void test() {
    GraphDependencyBuilder<Node> builder = new GraphDependencyBuilder<>(e -> e.children.stream().collect(Collectors.toSet()));
    Node e3 = new Node("e3");
    Node f3 = new Node("f3");
    Node g3 = new Node("g3");
    Node c2 = new Node(Set.of(f3), "c2");
    Node d2 = new Node(Set.of(g3), "d2");
    Node a1 = new Node(Set.of(e3, c2, d2), "a1");
    Node b1 = new Node(Set.of(d2), "b1");

    Graph<GraphDependencyBuilder.NodeWithId<Node>> graph = builder.build(List.of(a1, b1));
    List<Node> nodes = new ArrayList<>();
    ExecutionPlan<Node, Void> plan = new ExecutionPlan<>(graph, (n, c) -> nodes.add(n));

    plan.execute(null);

    Assertions.assertThat(plan.getLeaves()).containsExactlyInAnyOrder(e3, f3, g3);
    Assertions.assertThat(plan.getRoots()).containsExactlyInAnyOrder(a1, b1);
    // could be d2, b1, c2, a1
    Assertions.assertThat(nodes).containsAnyOf(d2, b1, c2, a1);
  }
}
