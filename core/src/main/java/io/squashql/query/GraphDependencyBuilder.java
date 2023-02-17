package io.squashql.query;

import java.util.*;
import java.util.function.Function;

public class GraphDependencyBuilder<N> {

  private final Function<N, Set<N>> dependencySupplier;

  public GraphDependencyBuilder(Function<N, Set<N>> dependencySupplier) {
    this.dependencySupplier = dependencySupplier;
  }

  public DependencyGraph<N> build(List<N> nodes) {
    DependencyGraph<N> graph = new DependencyGraph();
    for (N node : nodes) {
      addToGraph(graph, node);
    }
    return graph;
  }

  private void addToGraph(DependencyGraph<N> graph, N node) {
    Set<N> dependencies = this.dependencySupplier.apply(node);
    graph.addNode(node);
    for (N dependency : dependencies) {
      graph.addNode(dependency);
      graph.putEdge(node, dependency);
      addToGraph(graph, dependency);
    }
  }
}
