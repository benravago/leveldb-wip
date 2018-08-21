package lib.leveldb;

import java.util.ServiceLoader;
import java.nio.file.ProviderNotFoundException;

public interface Factory {

    DB.Builder newBuilder();

    static DB.Builder builder() {
        for (var factory : ServiceLoader.load(Factory.class)) {
            var builder = factory.newBuilder();
            if (builder != null) return builder;
        }
        throw new ProviderNotFoundException("no DB.Builder providers available");
    }
}
