package bsd.leveldb.io;

import java.util.Map.Entry;
import java.util.LinkedHashMap;
import java.util.function.Consumer;

public class LruHashMap<K,V> extends LinkedHashMap<K,V> implements Cache<K,V> {

    private final int maximumCapacity;
    private volatile Consumer<Entry<K,V>> notify;

    public LruHashMap(int maximumCapacity) {
        this(16, 0.75f, maximumCapacity);
    }

    public LruHashMap(int initialCapacity, float loadFactor, int maximumCapacity) {
        super((initialCapacity < maximumCapacity ? initialCapacity : maximumCapacity), loadFactor, true);
        this.maximumCapacity = maximumCapacity;
    }

    @Override
    public LruHashMap onDelete(Consumer<Entry<K,V>> notify) {
        this.notify = notify;
        return this;
    }

    @Override
    protected boolean removeEldestEntry(Entry<K,V> eldest) {
        if (size() > maximumCapacity) {
            if (notify != null) {
                notify.accept(eldest);
            }
            return true;
        } else {
            return false;
        }
    }
}
