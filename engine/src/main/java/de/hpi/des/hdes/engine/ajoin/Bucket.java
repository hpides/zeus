package de.hpi.des.hdes.engine.ajoin;

import de.hpi.des.hdes.engine.window.Window;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.Value;

@Value
public class Bucket<K, V> {

  private final UUID id;
  private final Map<K, Set<V>> set;
  private final Window window;

  public Bucket(final Map<K, Set<V>> set, final Window window) {
    this.set = set;
    this.window = window;
    this.id = UUID.randomUUID();
  }

  public Bucket(final K key, final Set<V> set, final Window window) {
    this(Map.of(key, set), window);
  }
}
