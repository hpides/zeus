package de.hpi.des.mpws2019.engine.operation;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.mpws2019.engine.execution.slot.QueueConnector;
import de.hpi.des.mpws2019.engine.execution.slot.QueueBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class StreamFilterTest {

  @Test
  void shouldFilterElements() {
    final List<Integer> result = new LinkedList<>();
    final StreamFilter<Integer> filter = new StreamFilter<>(i -> i % 2 == 0);
    filter.init(result::add);

    List.of(-2, 5, 0, 1, 3, 10, 2, 15)
        .forEach(filter::process);

    assertThat(result).containsExactly(-2, 0, 10, 2);
  }

}