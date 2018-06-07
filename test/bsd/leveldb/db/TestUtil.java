package bsd.leveldb.db;

import java.util.Arrays;

import java.text.SimpleDateFormat;
import java.util.Date;

import java.io.PrintStream;

import java.io.IOException;
import java.io.UncheckedIOException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

import bsd.leveldb.Slice;

final class TestUtil {

    static Slice s(String s) { return new Slice(s.getBytes()); }
    static String s(Slice s) { return new String(s.data,s.offset,s.length); }

    static void tmpDir(String dir) { System.setProperty("java.io.tmpdir",dir); }
    static String tmpDir() { return System.getProperty("java.io.tmpdir"); }

    static SimpleDateFormat timestamp = new SimpleDateFormat(".yyyyMMddHHmmss.SSS");
    static String timestamp() { return timestamp.format(new Date()); }

    static void preserve(String dir, String id) {
        try {
            Path src = Paths.get(dir);
            if (!Files.exists(src)) return;
            String n = src.getFileName().toString();
            Path dst = Files.createDirectory(src.getParent().resolve(n+timestamp()));
            System.out.println(src.toString() + " -> " + dst.toString());
            Files.list(src).forEach((f) -> {
                try {
                    Files.createLink(dst.resolve(f.getFileName()), f);
                }
                catch (IOException e) { throw new UncheckedIOException(e); }
            });
        }
        catch (IOException e) { throw new UncheckedIOException(e); }
    }

    static Throwable exec(Runnable run) {
        try {run.run(); return null; }
        catch (Throwable t) { return t; }
    }

    static void expect(Class<?> f, String c, String m, Throwable t) {
        if (f.isInstance(t)) {
            StackTraceElement e = t.getStackTrace()[0];
            if (isClass(c,e) && isMethod(m,e)) return;
        }
        throw (t instanceof RuntimeException) ? (RuntimeException)t : new RuntimeException(t);
    }

    static void expect(Class<?> f, Runnable r) { expect(f,null,null,exec(r)); }
    static void expect(Class<?> f, String c, String m, Runnable r) { expect(f,c,m,exec(r)); }

    static boolean isClass(String c, StackTraceElement e) { return c == null || e.getClassName().startsWith(c); }
    static boolean isMethod(String m, StackTraceElement e) { return m == null || e.getMethodName().equals(m); }

    static boolean fault(Throwable t) {
        t.printStackTrace(); return false;
    }

    static String string(int n, char val) {
        char[] a = new char[n];
        Arrays.fill(a, val);
        return new String(a);
    }

    static PrintStream stdout = System.out;
    static PrintStream stderr = System.err;

    static void fprintf(PrintStream f, String format, Object... args) {
        f.format(format, args);
    }

    static String hex(byte b) { return new String(new byte[]{hex[(b>>4)&0x0f],hex[b&0x0f]}); }
    static byte[] hex = {0x30,0x31,0x32,0x33,0x34,0x35,0x36,0x37,0x38,0x39,0x61,0x62,0x63,0x64,0x65,0x66};

    static void dump(byte[] b) {
        dump(b,0,b.length);
    }

    static void dump(byte[] b, int off, int len) {
        dump(System.out,b,off,len);
    }

    static void dump(PrintStream out, byte[] b, int off, int len) {
        byte[] a = new byte[80];
        int limit = off + len;
        int p = off & 0x0f;
        while (off < limit) {
            Arrays.fill(a,(byte)0x20);
            a[60+p] = '|';
            int i = 8;
            int j = off & 0x7ffffff0;
            while (i > 0) {
                a[--i] = hex[j&0x0f]; j >>>= 4;
            }
            while (p < 16 && off < limit) {
                i = p * 3 + (p < 8 ? 0 : 1);
                j = b[off] & 0x0ff;
                a[10+i] = hex[j>>>4];
                a[11+i] = hex[j&0x0f];
                a[61+p] = (byte)(j < 0x20 ? '.' : j > 0x7e ? '.' : j);
                p++; off++;
            }
            j = 61+p;
            a[j] = '|';
            a[j+1] = '\n';
            out.write(a,0,j+2);
            p = 0;
        }
    }

    static String x(int i) { return " "+Integer.toHexString(i); }
    static String x(long l) { return " "+Long.toHexString(l); }


// // Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// // Use of this source code is governed by a BSD-style license that can be
// // found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// #include "util/testharness.h"
//
// #include <string>
// #include <stdlib.h>
// #include <sys/stat.h>
// #include <sys/types.h>
//
// namespace leveldb {
// namespace test {
//
// namespace {
// struct Test {
//   const char* base;
//   const char* name;
//   void (*func)();
// };
// std::vector<Test>* tests;
// }
//
// bool RegisterTest(const char* base, const char* name, void (*func)()) {
//   if (tests == NULL) {
//     tests = new std::vector<Test>;
//   }
//   Test t;
//   t.base = base;
//   t.name = name;
//   t.func = func;
//   tests->push_back(t);
//   return true;
// }
//
// int RunAllTests() {
//   const char* matcher = getenv("LEVELDB_TESTS");
//
//   int num = 0;
//   if (tests != NULL) {
//     for (size_t i = 0; i < tests->size(); i++) {
//       const Test& t = (*tests)[i];
//       if (matcher != NULL) {
//         std::string name = t.base;
//         name.push_back('.');
//         name.append(t.name);
//         if (strstr(name.c_str(), matcher) == NULL) {
//           continue;
//         }
//       }
//       fprintf(stderr, "==== Test %s.%s\n", t.base, t.name);
//       (*t.func)();
//       ++num;
//     }
//   }
//   fprintf(stderr, "==== PASSED %d tests\n", num);
//   return 0;
// }

// int RandomSeed() {
//   const char* env = getenv("TEST_RANDOM_SEED");
//   int result = (env != NULL ? atoi(env) : 301);
//   if (result <= 0) {
//     result = 301;
//   }
//   return result;
// }
//
// }  // namespace test
// }  // namespace leveldb
// // Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// // Use of this source code is governed by a BSD-style license that can be
// // found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// #ifndef STORAGE_LEVELDB_UTIL_TESTHARNESS_H_
// #define STORAGE_LEVELDB_UTIL_TESTHARNESS_H_
//
// #include <stdio.h>
// #include <stdlib.h>
// #include <sstream>
// #include "leveldb/env.h"
// #include "leveldb/slice.h"
// #include "util/random.h"
//
// namespace leveldb {
// namespace test {
//
// // Run some of the tests registered by the TEST() macro.  If the
// // environment variable "LEVELDB_TESTS" is not set, runs all tests.
// // Otherwise, runs only the tests whose name contains the value of
// // "LEVELDB_TESTS" as a substring.  E.g., suppose the tests are:
// //    TEST(Foo, Hello) { ... }
// //    TEST(Foo, World) { ... }
// // LEVELDB_TESTS=Hello will run the first test
// // LEVELDB_TESTS=o     will run both tests
// // LEVELDB_TESTS=Junk  will run no tests
// //
// // Returns 0 if all tests pass.
// // Dies or returns a non-zero value if some test fails.
// extern int RunAllTests();
//
// // Return the directory to use for temporary storage.
// extern std::string TmpDir();
//
// // Return a randomization seed for this run.  Typically returns the
// // same number on repeated invocations of this binary, but automated
// // runs may be able to vary the seed.
// extern int RandomSeed();
//
// // An instance of Tester is allocated to hold temporary state during
// // the execution of an assertion.
// class Tester {
//  private:
//   bool ok_;
//   const char* fname_;
//   int line_;
//   std::stringstream ss_;
//
//  public:
//   Tester(const char* f, int l)
//       : ok_(true), fname_(f), line_(l) {
//   }
//
//   ~Tester() {
//     if (!ok_) {
//       fprintf(stderr, "%s:%d:%s\n", fname_, line_, ss_.str().c_str());
//       exit(1);
//     }
//   }
//
//   Tester& Is(bool b, const char* msg) {
//     if (!b) {
//       ss_ << " Assertion failure " << msg;
//       ok_ = false;
//     }
//     return *this;
//   }
//
//   Tester& IsOk(const Status& s) {
//     if (!s.ok()) {
//       ss_ << " " << s.ToString();
//       ok_ = false;
//     }
//     return *this;
//   }
//
// #define BINARY_OP(name,op)                              \
//   template <class X, class Y>                           \
//   Tester& name(const X& x, const Y& y) {                \
//     if (! (x op y)) {                                   \
//       ss_ << " failed: " << x << (" " #op " ") << y;    \
//       ok_ = false;                                      \
//     }                                                   \
//     return *this;                                       \
//   }
//
//   BINARY_OP(IsEq, ==)
//   BINARY_OP(IsNe, !=)
//   BINARY_OP(IsGe, >=)
//   BINARY_OP(IsGt, >)
//   BINARY_OP(IsLe, <=)
//   BINARY_OP(IsLt, <)
// #undef BINARY_OP
//
//   // Attach the specified value to the error message if an error has occurred
//   template <class V>
//   Tester& operator<<(const V& value) {
//     if (!ok_) {
//       ss_ << " " << value;
//     }
//     return *this;
//   }
// };
//
// #define ASSERT_TRUE(c) ::leveldb::test::Tester(__FILE__, __LINE__).Is((c), #c)
// #define ASSERT_OK(s) ::leveldb::test::Tester(__FILE__, __LINE__).IsOk((s))
// #define ASSERT_EQ(a,b) ::leveldb::test::Tester(__FILE__, __LINE__).IsEq((a),(b))
// #define ASSERT_NE(a,b) ::leveldb::test::Tester(__FILE__, __LINE__).IsNe((a),(b))
// #define ASSERT_GE(a,b) ::leveldb::test::Tester(__FILE__, __LINE__).IsGe((a),(b))
// #define ASSERT_GT(a,b) ::leveldb::test::Tester(__FILE__, __LINE__).IsGt((a),(b))
// #define ASSERT_LE(a,b) ::leveldb::test::Tester(__FILE__, __LINE__).IsLe((a),(b))
// #define ASSERT_LT(a,b) ::leveldb::test::Tester(__FILE__, __LINE__).IsLt((a),(b))
//
// #define TCONCAT(a,b) TCONCAT1(a,b)
// #define TCONCAT1(a,b) a##b
//
// #define TEST(base,name)                                                 \
// class TCONCAT(_Test_,name) : public base {                              \
//  public:                                                                \
//   void _Run();                                                          \
//   static void _RunIt() {                                                \
//     TCONCAT(_Test_,name) t;                                             \
//     t._Run();                                                           \
//   }                                                                     \
// };                                                                      \
// bool TCONCAT(_Test_ignored_,name) =                                     \
//   ::leveldb::test::RegisterTest(#base, #name, &TCONCAT(_Test_,name)::_RunIt); \
// void TCONCAT(_Test_,name)::_Run()
//
// // Register the specified test.  Typically not used directly, but
// // invoked via the macro expansion of TEST.
// extern bool RegisterTest(const char* base, const char* name, void (*func)());
//
//
// }  // namespace test
// }  // namespace leveldb
//
// #endif  // STORAGE_LEVELDB_UTIL_TESTHARNESS_H_
// // Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// // Use of this source code is governed by a BSD-style license that can be
// // found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// #include "util/testutil.h"
//
// #include "util/random.h"
//
// namespace leveldb {
// namespace test {
//
// Slice RandomString(Random* rnd, int len, std::string* dst) {
//   dst->resize(len);
//   for (int i = 0; i < len; i++) {
//     (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));   // ' ' .. '~'
//   }
//   return Slice(*dst);
// }
//
// std::string RandomKey(Random* rnd, int len) {
//   // Make sure to generate a wide variety of characters so we
//   // test the boundary conditions for short-key optimizations.
//   static const char kTestChars[] = {
//     '\0', '\1', 'a', 'b', 'c', 'd', 'e', '\xfd', '\xfe', '\xff'
//   };
//   std::string result;
//   for (int i = 0; i < len; i++) {
//     result += kTestChars[rnd->Uniform(sizeof(kTestChars))];
//   }
//   return result;
// }
//
//
// extern Slice CompressibleString(Random* rnd, double compressed_fraction,
//                                 size_t len, std::string* dst) {
//   int raw = static_cast<int>(len * compressed_fraction);
//   if (raw < 1) raw = 1;
//   std::string raw_data;
//   RandomString(rnd, raw, &raw_data);
//
//   // Duplicate the random data until we have filled "len" bytes
//   dst->clear();
//   while (dst->size() < len) {
//     dst->append(raw_data);
//   }
//   dst->resize(len);
//   return Slice(*dst);
// }
//
// }  // namespace test
// }  // namespace leveldb
// // Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// // Use of this source code is governed by a BSD-style license that can be
// // found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// #ifndef STORAGE_LEVELDB_UTIL_TESTUTIL_H_
// #define STORAGE_LEVELDB_UTIL_TESTUTIL_H_
//
// #include "leveldb/env.h"
// #include "leveldb/slice.h"
// #include "util/random.h"
//
// namespace leveldb {
// namespace test {
//
// // Store in *dst a random string of length "len" and return a Slice that
// // references the generated data.
// extern Slice RandomString(Random* rnd, int len, std::string* dst);
//
// // Return a random key with the specified length that may contain interesting
// // characters (e.g. \x00, \xff, etc.).
// extern std::string RandomKey(Random* rnd, int len);
//
// // Store in *dst a string of length "len" that will compress to
// // "N*compressed_fraction" bytes and return a Slice that references
// // the generated data.
// extern Slice CompressibleString(Random* rnd, double compressed_fraction,
//                                 size_t len, std::string* dst);
//
// // A wrapper that allows injection of errors.
// class ErrorEnv : public EnvWrapper {
//  public:
//   bool writable_file_error_;
//   int num_writable_file_errors_;
//
//   ErrorEnv() : EnvWrapper(Env::Default()),
//                writable_file_error_(false),
//                num_writable_file_errors_(0) { }
//
//   virtual Status NewWritableFile(const std::string& fname,
//                                  WritableFile** result) {
//     if (writable_file_error_) {
//       ++num_writable_file_errors_;
//       *result = NULL;
//       return Status::IOError(fname, "fake error");
//     }
//     return target()->NewWritableFile(fname, result);
//   }
//
//   virtual Status NewAppendableFile(const std::string& fname,
//                                    WritableFile** result) {
//     if (writable_file_error_) {
//       ++num_writable_file_errors_;
//       *result = NULL;
//       return Status::IOError(fname, "fake error");
//     }
//     return target()->NewAppendableFile(fname, result);
//   }
// };
//
}
// }  // namespace leveldb
//
// #endif  // STORAGE_LEVELDB_UTIL_TESTUTIL_H_
