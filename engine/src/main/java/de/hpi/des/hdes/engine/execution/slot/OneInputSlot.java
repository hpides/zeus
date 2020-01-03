package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.execution.connector.Buffer;
import de.hpi.des.hdes.engine.operation.Collector;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import java.util.UUID;

public class OneInputSlot<IN, OUT> extends Slot {

  private final OneInputOperator<IN, OUT> operator;
  private final Buffer<IN> input;

  public OneInputSlot(final OneInputOperator<IN, OUT> operator,
                      final Buffer<IN> input,
                      final Collector<OUT> output,
                      final UUID topologyNodeId) {
    super(topologyNodeId);
    this.operator = operator;
    this.input = input;
    this.operator.init(output);
  }

  public void runStep() {
    this.operator.tick();
    final IN in = this.input.poll();
    if (in != null) {
      this.operator.process(in);
    }
  }

  @Override
  public void tick() {
    this.operator.tick();
  }
}
