package lib.util;

import java.util.Map;
import java.util.function.Consumer;

public interface LruMap<K,V> extends Map<K,V> {
    LruMap<K,V> onDelete(Consumer<Map.Entry<K,V>> notify);   
}
