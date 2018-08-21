package lib.util.concurrent;

import java.util.concurrent.locks.ReentrantLock;

public class MutexLock extends ReentrantLock implements AutoCloseable {

    public MutexLock open() {
        lock();
        return this;
    }

    @Override
    public void close() {
        unlock();
    }

    private static final long serialVersionUID = 1L;
}
/*

    mutex.open(); // or mutex.lock();
    try (mutex) {
        ...
    }

*/