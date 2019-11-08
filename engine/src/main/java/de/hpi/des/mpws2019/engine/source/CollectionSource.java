package de.hpi.des.mpws2019.engine.source;

import java.util.Collection;
import java.util.Iterator;

public class CollectionSource<V> implements Source<V> {
    private final Iterator<V> collection;

    public CollectionSource(final Collection<V> collection) {
        this.collection = collection.iterator();
    }

    @Override
    public V poll() {
        if (this.collection.hasNext()) {
            return this.collection.next();
        } else {
            return null;
        }
    }
}
