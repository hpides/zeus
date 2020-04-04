package de.hpi.des.hdes.engine.shared.join;

import de.hpi.des.hdes.engine.window.Window;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.Value;

/**
 * A Bucket contains a mapping of index to values for a window.
 *
 * @param <K> the type of the key (index)
 * @param <V> the type of the value in the set
 */
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
