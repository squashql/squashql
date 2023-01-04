package io.squashql.query;

import com.google.common.graph.Graph;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntSupplier;

public class GraphDependencyBuilder<N> {

  private final IntSupplier id = new AtomicInteger()::getAndIncrement;
  private final Map<N, Integer> idByNode = new HashMap<>();
  private final Function<N, Set<N>> dependencySupplier;

  public GraphDependencyBuilder(Function<N, Set<N>> dependencySupplier) {
    this.dependencySupplier = dependencySupplier;
  }

  public Graph<NodeWithId<N>> build(List<N> nodes) {
    Function<N, NodeWithId<N>> transformer = m -> new NodeWithId(m, this.idByNode.computeIfAbsent(m, k -> this.id.getAsInt()));
    MutableGraph<NodeWithId<N>> graph = GraphBuilder.directed().build();
    for (N node : nodes) {
      this.idByNode.computeIfAbsent(node, k -> this.id.getAsInt());
      addToGraph(graph, transformer, node);
    }
    return graph;
  }

  private void addToGraph(MutableGraph<NodeWithId<N>> graph, Function<N, NodeWithId<N>> transformer, N node) {
    Set<N> dependencies = this.dependencySupplier.apply(node);
    graph.addNode(transformer.apply(node));
    for (N dependency : dependencies) {
      graph.addNode(transformer.apply(dependency));
      graph.putEdge(transformer.apply(node), transformer.apply(dependency));
      addToGraph(graph, transformer, dependency);
    }
  }

  public static class NodeWithId<N> {

    final int id;
    final N node;

    NodeWithId(N node, int id) {
      this.node = node;
      this.id = id;
    }

    @Override
    public String toString() {
      return "N{" + "id=" + this.id + ", node=" + this.node + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NodeWithId that = (NodeWithId) o;
      return this.node.equals(that.node);
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.node);
    }
  }
}
