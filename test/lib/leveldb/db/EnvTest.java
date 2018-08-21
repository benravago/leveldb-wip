package bsd.leveldb.db;

import bsd.leveldb.io.MutexLock;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import org.junit.Test;

public class EnvTest {
// // Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// // Use of this source code is governed by a BSD-style license that can be
// // found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// #include "leveldb/env.h"
//
// #include "port/port.h"
// #include "util/testharness.h"
//
// namespace leveldb {

    static final int kDelayMicros = 100000;
    static final int kReadOnlyFileLimit = 4;
    static final int kMMapLimit = 4;

// class EnvTest {
//  private:
//   port::Mutex mu_;
//   std::string events_;
//
//  public:
    Env env = new Env(){};
//   EnvTest() : env_(Env::Default()) { }
// };

    void setBool(AtomicBoolean ptr) {
        ptr.set(true);
    }

    @Test
    public void EnvTest_RunImmediately() throws Exception {
        AtomicBoolean result = new AtomicBoolean();
        Future<?> called = env.schedule(() -> { setBool(result); });
        env.sleepForMicroseconds(kDelayMicros);
        assertTrue(called.isDone());
        assertTrue(result.get());
    }

    class CB {
        AtomicInteger lastIdPtr; // Pointer to shared slot
        int id; // Order# for the execution of this callback

        CB(AtomicInteger p, int i) { lastIdPtr = p; id = i; }
    }
    void cbRun(CB cb) {
        int cur = cb.lastIdPtr.get();
        assertEquals(cb.id - 1, cur);
        cb.lastIdPtr.set(cb.id);
    }

    @Test
    public void EnvTest_RunMany() {
        AtomicInteger lastId = new AtomicInteger();

        // Schedule in different order than start time
        CB cb1 = new CB(lastId, 1);
        CB cb2 = new CB(lastId, 2);
        CB cb3 = new CB(lastId, 3);
        CB cb4 = new CB(lastId, 4);
        env.schedule(() -> { cbRun(cb1); });
        env.schedule(() -> { cbRun(cb2); });
        env.schedule(() -> { cbRun(cb3); });
        env.schedule(() -> { cbRun(cb4); });

        env.sleepForMicroseconds(kDelayMicros);
        int cur = lastId.get();
        assertEquals(4, cur);
    }

    class State {
        MutexLock mu;
        int val;
        int numRunning;
    }

    static void threadBody(State s) {
        s.mu.lock();
        s.val += 1;
        s.numRunning -= 1;
        s.mu.unlock();
    }

    @Test
    public void EnvTest_StartThread() {
        State state = new State();
        state.mu = new MutexLock();
        state.val = 0;
        state.numRunning = 3;
        for (int i = 0; i < 3; i++) {
            env.threadFactory()
               .newThread(() -> { threadBody(state); })
               .start();
        }
        while (true) {
            state.mu.lock();
            int num = state.numRunning;
            state.mu.unlock();
            if (num == 0) {
                break;
            }
            env.sleepForMicroseconds(kDelayMicros);
        }
        assertEquals(state.val, 3);
    }

}
//
// int main(int argc, char** argv) {
//   return leveldb::test::RunAllTests();
// }
