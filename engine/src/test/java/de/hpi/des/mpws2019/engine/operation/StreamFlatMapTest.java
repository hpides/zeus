package de.hpi.des.mpws2019.engine.operation;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.mpws2019.engine.execution.slot.QueueConnector;
import de.hpi.des.mpws2019.engine.execution.slot.QueueBuffer;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class StreamFlatMapTest {

  @Test
  void shouldFlatMapElements() {
    final QueueConnector<Character> collector = new QueueConnector<>();
    final QueueBuffer queueBuffer = collector.addQueueBuffer(UUID.fromString("00000000-0000-0000-0000-000000000000"));
    final StreamFlatMap<String, Character> filter = new StreamFlatMap<>(string -> string.chars()
        .mapToObj(c -> (char) c)
        .collect(Collectors.toList()));
    
    filter.init(collector);

    List.of("hel", "lo")
        .forEach(filter::process);

    assertThat(queueBuffer.getQueue()).containsExactly('h', 'e', 'l', 'l', 'o');
  }
}