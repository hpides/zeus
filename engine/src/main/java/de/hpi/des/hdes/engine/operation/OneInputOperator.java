package de.hpi.des.hdes.engine.operation;

import org.jetbrains.annotations.NotNull;

public interface OneInputOperator<IN, OUT> extends Operator<OUT> {

  void process(@NotNull IN in);
}
