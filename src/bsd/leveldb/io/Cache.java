package bsd.leveldb.io;

import java.util.Map;
import java.util.function.Consumer;

public interface Cache<K,V> extends Map<K,V> {
    Cache onDelete(Consumer<Entry<K,V>> notify);
}
