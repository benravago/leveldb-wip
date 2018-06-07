package bsd.leveldb.io;

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
}
