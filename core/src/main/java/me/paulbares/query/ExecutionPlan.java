package me.paulbares.query;

import com.google.common.graph.Graph;
import me.paulbares.query.GraphDependencyBuilder.NodeWithId;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

public class ExecutionPlan<N, Context> {

  private final BiConsumer<N, Context> consumer;
  private final Graph<NodeWithId<N>> graph;
  private final Set<N> processed = new HashSet<>();
  private final Set<N> roots = new HashSet<>();
  private final Set<N> leaves = new HashSet<>();

  public ExecutionPlan(Graph<NodeWithId<N>> graph, BiConsumer<N, Context> consumer) {
    this.graph = graph;
    this.consumer = consumer;

    Set<NodeWithId<N>> nodes = this.graph.nodes();
    for (NodeWithId<N> node : nodes) {
      if (this.graph.outDegree(node) == 0) {
        this.leaves.add(node.node);
      }

      if (this.graph.inDegree(node) == 0) {
        this.roots.add(node.node);
      }
    }
  }

  public void execute(Context context) {
    this.processed.addAll(this.leaves);

    for (N node : this.roots) {
      executeRecursively(node, context);
    }
  }

  private void executeRecursively(N node, Context context) {
    boolean hasBeenProcessed = this.processed.contains(node);
    if (!hasBeenProcessed) {
      Set<NodeWithId<N>> successors = this.graph.successors(new NodeWithId(node, -1));
      for (NodeWithId<N> successor : successors) {
        if (!this.processed.contains(successor.node)) {
          executeRecursively(successor.node, context);
        }
      }
      this.consumer.accept(node, context);
      this.processed.add(node);
    }
  }

  public Set<N> getRoots() {
    return this.roots;
  }

  public Set<N> getLeaves() {
    return this.leaves;
  }
}