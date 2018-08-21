package lib.util;

import java.util.LinkedHashMap;
import java.util.function.Consumer;

public class LruHashMap<K,V> extends LinkedHashMap<K,V> implements LruMap<K,V> {

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
    public LruHashMap<K,V> onDelete(Consumer<Entry<K,V>> notify) {
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

    private static final long serialVersionUID = 1L;
}
