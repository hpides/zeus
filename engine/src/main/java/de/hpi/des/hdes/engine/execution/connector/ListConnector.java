package de.hpi.des.hdes.engine.execution.connector;

import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.graph.SinkNode;
import de.hpi.des.hdes.engine.operation.Collector;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import de.hpi.des.hdes.engine.operation.Sink;
import java.util.concurrent.CopyOnWriteArrayList;

public class ListConnector<T> implements Collector<T> {
  private final CopyOnWriteArrayList<OneInputOperator<T, ?>> outOps = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<Sink<T>> outSinks = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<Buffer<T>> outBuffers = new CopyOnWriteArrayList<>();

  private ListConnector() {
  }

  public static <T> ListConnector<T> create() {
    return new ListConnector<T>();
  }

  public void addFunction(final Node node, final OneInputOperator<T,?> processFunc) {
    // todo: we ignore node for now. We could add it to a list and map the lists to each other later
    outOps.add(processFunc);
  }

  public void addFunction(final Node node, final Sink<T> sink) {
    // todo: we ignore node for now. We could add it to a list and map the lists to each other later
    outSinks.add(sink);
  }

  public Buffer<T> addBuffer(final Node node) {
    Buffer<T> buffer = Buffer.create();
    outBuffers.add(buffer);
    return buffer;
  }

    @Override
  public void collect(T t) {
    for(var op: outOps){
      op.process(t);
    }
    for(var sink: outSinks){
      sink.process(t);
    }
    for(var buf: outBuffers){
      buf.add(t);
    }
  }
  @Override
  public void tick(){
    for(var op: outOps){
      op.tick();
    }
    for(var buf: outBuffers){
      buf.flushIfTimeout();
    }
  }


}
