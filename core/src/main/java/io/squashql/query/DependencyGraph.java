package io.squashql.query;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntSupplier;

public class DependencyGraph<N> {

  private final IntSupplier idGenerator = new AtomicInteger()::getAndIncrement;
  private final Map<N, Integer> idByNode = new HashMap<>();
  private final Function<N, NodeWithId<N>> transformer = node -> new NodeWithId<>(node, this.idByNode.computeIfAbsent(node, __ -> this.idGenerator.getAsInt()));
  private final MutableGraph<NodeWithId<N>> graph;

  public DependencyGraph() {
    this.graph = GraphBuilder.directed().build();
  }

  public boolean addNode(N node) {
    return this.graph.addNode(this.transformer.apply(node));
  }

  public boolean putEdge(N node1, N node2) {
    return this.graph.putEdge(this.transformer.apply(node1), this.transformer.apply(node2));
  }

  public Set<NodeWithId<N>> nodes() {
    return this.graph.nodes();
  }

  public int outDegree(NodeWithId<N> node) {
    return this.graph.outDegree(node);
  }

  public int inDegree(NodeWithId<N> node) {
    return this.graph.inDegree(node);
  }

  public Set<NodeWithId<N>> successors(NodeWithId<N> node) {
    return this.graph.successors(node);
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
      NodeWithId<?> that = (NodeWithId<?>) o;
      return this.id == that.id && Objects.equals(this.node, that.node);
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.id, this.node);
    }
  }
}
