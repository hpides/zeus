package de.hpi.des.mpws2019.engine.operation;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.mpws2019.engine.execution.slot.QueueBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

class StreamFilterTest {

  @Test
  void shouldFilterElements() {
    final QueueBuffer<Integer> collector = new QueueBuffer<>();
    final StreamFilter<Integer> filter = new StreamFilter<>(i -> i % 2 == 0);
    filter.init(collector);

    List.of(-2, 5, 0, 1, 3, 10, 2, 15)
        .forEach(filter::process);

    assertThat(collector.getQueue()).containsExactly(-2, 0, 10, 2);
  }

}