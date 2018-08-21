package lib.leveldb.db;

import lib.leveldb.Slice;
import lib.leveldb.Status;
import static lib.leveldb.db.DbFormat.*;
import static lib.leveldb.db.FileName.*;
import static lib.leveldb.db.FileName.FileType.*;
// import lib.leveldb.db.Info;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lib.leveldb.DB.FilterPolicy;
import lib.leveldb.Env;

// We recover the contents of the descriptor from the other files we find.
// (1) Any log files are first converted to tables
// (2) We scan every table to compute
//     (a) smallest/largest for the table
//     (b) largest sequence number in the table
// (3) We generate descriptor contents:
//      - log number is set to zero
//      - next-file-number is set to 1 + largest file number we found
//      - last-sequence-number is set to largest sequence# found across
//        all tables (see 2c)
//      - compaction pointers are cleared
//      - every table file is added at level 0
//
// Possible optimization 1:
//   (a) Compute total size and use to pick appropriate max-level M
//   (b) Sort tables by largest sequence# in the table
//   (c) For each table: if it overlaps earlier table, place in level-0,
//       else place in level-M.
// Possible optimization 2:
//   Store per-table metadata (smallest, largest, largest-seq#, ...)
//   in the table's meta section to speed up ScanTable.

class Repair implements Closeable {

//  private:
    class TableInfo {
        FileMetaData meta;
        long maxSequence;
    }
// 
    DbImpl db; // TODO:
    Path dbname;
    Env env;
    InternalKeyComparator icmp;
    FilterPolicy ipolicy;
//   Options const options_;
//   bool owns_info_log_;
//   bool owns_cache_;
//   TableCache* table_cache_;
//   VersionEdit edit_;

    List<Path> manifests = new ArrayList<>();
    List<Long> tableNumbers = new ArrayList<>(); //   std::vector<uint64_t> table_numbers_;
    List<Long> logs = new ArrayList<>(); //   std::vector<uint64_t> logs_;
    List<TableInfo> tables = new ArrayList<>();
    long nextFileNumber;

    Repair(String dbname, Env env, Comparator<Slice> cmp, FilterPolicy policy) {

// class Repairer {
//  public:
//   Repairer(const std::string& dbname, const Options& options)
        this.dbname = Paths.get(dbname);
//      this.env = DbFactory.environment(env);
//      icmp = DbFactory.internalComparator(cmp);
        ipolicy = policy;
//         options_(SanitizeOptions(dbname, &icmp_, &ipolicy_, options)),
//         owns_info_log_(options_.info_log != options.info_log),
//         owns_cache_(options_.block_cache != options.block_cache),
        nextFileNumber = 1;
//     // TableCache can be small since we expect each table to be opened once.
//     table_cache_ = new TableCache(dbname_, &options_, 10);
    }

    @Override
    public void close() {
//   ~Repairer() {
//     delete table_cache_;
//     if (owns_info_log_) {
//       delete options_.info_log;
//     }
//     if (owns_cache_) {
//       delete options_.block_cache;
//     }
    }

    void run() { // Status Run() {
        boolean ok = findFiles(); // Status status = FindFiles();
        if (ok) { //     if (status.ok()) {
            convertLogFilesToTables();
            extractMetaData();
            writeDescriptor();  //       status = WriteDescriptor();
//     }
//     if (status.ok()) {
            long bytes = 0; //       unsigned long long bytes = 0;
            for (int i = 0; i < tables.size(); i++) {
                bytes += tables.get(i).meta.fileSize;
            }
            db.info(
                "**** Repaired leveldb {0}; " +
                "recovered {1,number} files; {2,number} bytes. " +
                "Some data may have been lost. " +
                "****",
                dbname.toString(),
                tables.size(),
                bytes);
        }
//     return status;
    }

    boolean findFiles() { //   Status FindFiles() {
        Path[] filenames = //     std::vector<std::string> filenames;
            env.getChildren(dbname); //     Status status = env_->GetChildren(dbname_, &filenames);
//     if (!status.ok()) {
//       return status;
//     }
        if (filenames.length < 1) {
            db.info("repair found no files"); // return Status::IOError(dbname_, "repair found no files");
            return false;
        }

//     uint64_t number;
//     FileType type;
        for (int i = 0; i < filenames.length; i++) {
            ParsedFileName p = parseFileName(filenames[i]); 
            if (p.type == kDescriptorFile) {
                manifests.add(filenames[i]);
            } else {
                if (p.number + 1 > nextFileNumber) {
                    nextFileNumber = p.number + 1;
                }
                if (p.type == kLogFile) {
                    logs.add(p.number);
                } else if (p.type == kTableFile) {
                    tableNumbers.add(p.number);
                } else {
                    // Ignore other files
                }
            }
        }
        return true; //     return status;
    }

    void convertLogFilesToTables() { // void ConvertLogFilesToTables() {
        for (int i = 0; i < logs.size(); i++) {
            Path logname = logFileName(dbname, logs.get(i));
            try {
                convertLogToTable(logs.get(i)); //       Status status = ConvertLogToTable(logs_[i]);
            }
            catch (Exception e) { //       if (!status.ok()) {
                db.info("Log #{0,number}: ignoring conversion error: {1}",
                         logs.get(i),
                         e.toString()); 
            }
            archiveFile(logname);
        }
    }


//     struct LogReporter : public log::Reader::Reporter {
//       Env* env;
//       Logger* info_log;
//       uint64_t lognum;
//       virtual void Corruption(size_t bytes, const Status& s) {
//         // We print error messages for corruption, but continue repairing.
//         Log(info_log, "Log #%llu: dropping %d bytes; %s",
//             (unsigned long long) lognum,
//             static_cast<int>(bytes),
//             s.ToString().c_str());
//       }
//     };

//     // Create the log reader.
//     LogReporter reporter;
//     reporter.env = env_;
//     reporter.info_log = options_.info_log;
//     reporter.lognum = log;
//     // We intentionally make log::Reader do checksumming so that
//     // corruptions cause entire commits to be skipped instead of
//     // propagating bad information (like overly large sequence
//     // numbers).
//     log::Reader reader(lfile, &reporter, false/*do not checksum*/,
//                        0/*initial_offset*/);

    
    void convertLogToTable(long log) throws IOException { //   Status ConvertLogToTable(uint64_t log) {
        // Open the log file
        Path logname = logFileName(dbname, log);
        InputStream lfile = //     SequentialFile* lfile;
            env.newSequentialFile(logname); // Status status = env_->NewSequentialFile(logname, &lfile);
//     if (!status.ok()) {
//       return status;
//     }

        // Create the log reader.
        LogReader reader = new LogReader(lfile);
//     LogReporter reporter;
//     reporter.env = env_;
//     reporter.info_log = options_.info_log;
//     reporter.lognum = log;
//     // We intentionally make log::Reader do checksumming so that
//     // corruptions cause entire commits to be skipped instead of
//     // propagating bad information (like overly large sequence
//     // numbers).
//     log::Reader reader(lfile, &reporter, false/*do not checksum*/,
//                        0/*initial_offset*/);

        // Read all the records and add to a memtable
//     std::string scratch;
//     Slice record;
//     WriteBatch batch;
//     MemTable* mem = new MemTable(icmp_);
//     mem->Ref();
//     int counter = 0;
//     while (reader.ReadRecord(&record, &scratch)) {
//       if (record.size() < 12) {
//         reporter.Corruption(
//             record.size(), Status::Corruption("log record too small"));
//         continue;
//       }
//       WriteBatchInternal::SetContents(&batch, record);
//       status = WriteBatchInternal::InsertInto(&batch, mem);
//       if (status.ok()) {
//         counter += WriteBatchInternal::Count(&batch);
//       } else {
//         Log(options_.info_log, "Log #%llu: ignoring %s",
//             (unsigned long long) log,
//             status.ToString().c_str());
//         status = Status::OK();  // Keep going with rest of file
//       }
//     }
//     delete lfile;

//     // Do not record a version edit for this conversion to a Table
//     // since ExtractMetaData() will also generate edits.
//     FileMetaData meta;
//     meta.number = next_file_number_++;
//     Iterator* iter = mem->NewIterator();
//     status = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
//     delete iter;
//     mem->Unref();
//     mem = NULL;
//     if (status.ok()) {
//       if (meta.file_size > 0) {
//         table_numbers_.push_back(meta.number);
//       }
//     }
//     Log(options_.info_log, "Log #%llu: %d ops saved to Table #%llu %s",
//         (unsigned long long) log,
//         counter,
//         (unsigned long long) meta.number,
//         status.ToString().c_str());
//     return status;
    }

    void extractMetaData() {
        for (int i = 0; i < tableNumbers.size(); i++) {
            scanTable(tableNumbers.get(i));
        }
    }

//   Iterator* NewTableIterator(const FileMetaData& meta) {
//     // Same as compaction iterators: if paranoid_checks are on, turn
//     // on checksum verification.
//     ReadOptions r;
//     r.verify_checksums = options_.paranoid_checks;
//     return table_cache_->NewIterator(r, meta.number, meta.file_size);
//   }

    void scanTable(long number) { //   void ScanTable(uint64_t number) {
//     TableInfo t;
//     t.meta.number = number;
//     std::string fname = TableFileName(dbname_, number);
//     Status status = env_->GetFileSize(fname, &t.meta.file_size);
//     if (!status.ok()) {
//       // Try alternate file name.
//       fname = SSTTableFileName(dbname_, number);
//       Status s2 = env_->GetFileSize(fname, &t.meta.file_size);
//       if (s2.ok()) {
//         status = Status::OK();
//       }
//     }
//     if (!status.ok()) {
//       ArchiveFile(TableFileName(dbname_, number));
//       ArchiveFile(SSTTableFileName(dbname_, number));
//       Log(options_.info_log, "Table #%llu: dropped: %s",
//           (unsigned long long) t.meta.number,
//           status.ToString().c_str());
//       return;
//     }
// 
//     // Extract metadata by scanning through table.
//     int counter = 0;
//     Iterator* iter = NewTableIterator(t.meta);
//     bool empty = true;
//     ParsedInternalKey parsed;
//     t.max_sequence = 0;
//     for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
//       Slice key = iter->key();
//       if (!ParseInternalKey(key, &parsed)) {
//         Log(options_.info_log, "Table #%llu: unparsable key %s",
//             (unsigned long long) t.meta.number,
//             EscapeString(key).c_str());
//         continue;
//       }
// 
//       counter++;
//       if (empty) {
//         empty = false;
//         t.meta.smallest.DecodeFrom(key);
//       }
//       t.meta.largest.DecodeFrom(key);
//       if (parsed.sequence > t.max_sequence) {
//         t.max_sequence = parsed.sequence;
//       }
//     }
//     if (!iter->status().ok()) {
//       status = iter->status();
//     }
//     delete iter;
//     Log(options_.info_log, "Table #%llu: %d entries %s",
//         (unsigned long long) t.meta.number,
//         counter,
//         status.ToString().c_str());
// 
//     if (status.ok()) {
//       tables_.push_back(t);
//     } else {
//       RepairTable(fname, t);  // RepairTable archives input file.
//     }
    }

//   void RepairTable(const std::string& src, TableInfo t) {
//     // We will copy src contents to a new table and then rename the
//     // new table over the source.
// 
//     // Create builder.
//     std::string copy = TableFileName(dbname_, next_file_number_++);
//     WritableFile* file;
//     Status s = env_->NewWritableFile(copy, &file);
//     if (!s.ok()) {
//       return;
//     }
//     TableBuilder* builder = new TableBuilder(options_, file);
// 
//     // Copy data.
//     Iterator* iter = NewTableIterator(t.meta);
//     int counter = 0;
//     for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
//       builder->Add(iter->key(), iter->value());
//       counter++;
//     }
//     delete iter;
// 
//     ArchiveFile(src);
//     if (counter == 0) {
//       builder->Abandon();  // Nothing to save
//     } else {
//       s = builder->Finish();
//       if (s.ok()) {
//         t.meta.file_size = builder->FileSize();
//       }
//     }
//     delete builder;
//     builder = NULL;
// 
//     if (s.ok()) {
//       s = file->Close();
//     }
//     delete file;
//     file = NULL;
// 
//     if (counter > 0 && s.ok()) {
//       std::string orig = TableFileName(dbname_, t.meta.number);
//       s = env_->RenameFile(copy, orig);
//       if (s.ok()) {
//         Log(options_.info_log, "Table #%llu: %d entries repaired",
//             (unsigned long long) t.meta.number, counter);
//         tables_.push_back(t);
//       }
//     }
//     if (!s.ok()) {
//       env_->DeleteFile(copy);
//     }
//   }

    void writeDescriptor() { //   Status WriteDescriptor() {
//     std::string tmp = TempFileName(dbname_, 1);
//     WritableFile* file;
//     Status status = env_->NewWritableFile(tmp, &file);
//     if (!status.ok()) {
//       return status;
//     }
// 
//     SequenceNumber max_sequence = 0;
//     for (size_t i = 0; i < tables_.size(); i++) {
//       if (max_sequence < tables_[i].max_sequence) {
//         max_sequence = tables_[i].max_sequence;
//       }
//     }
// 
//     edit_.SetComparatorName(icmp_.user_comparator()->Name());
//     edit_.SetLogNumber(0);
//     edit_.SetNextFile(next_file_number_);
//     edit_.SetLastSequence(max_sequence);
// 
//     for (size_t i = 0; i < tables_.size(); i++) {
//       // TODO(opt): separate out into multiple levels
//       const TableInfo& t = tables_[i];
//       edit_.AddFile(0, t.meta.number, t.meta.file_size,
//                     t.meta.smallest, t.meta.largest);
//     }
// 
//     //fprintf(stderr, "NewDescriptor:\n%s\n", edit_.DebugString().c_str());
//     {
//       log::Writer log(file);
//       std::string record;
//       edit_.EncodeTo(&record);
//       status = log.AddRecord(record);
//     }
//     if (status.ok()) {
//       status = file->Close();
//     }
//     delete file;
//     file = NULL;
// 
//     if (!status.ok()) {
//       env_->DeleteFile(tmp);
//     } else {
//       // Discard older manifests
//       for (size_t i = 0; i < manifests_.size(); i++) {
//         ArchiveFile(dbname_ + "/" + manifests_[i]);
//       }
// 
//       // Install new manifest
//       status = env_->RenameFile(tmp, DescriptorFileName(dbname_, 1));
//       if (status.ok()) {
//         status = SetCurrentFile(env_, dbname_, 1);
//       } else {
//         env_->DeleteFile(tmp);
//       }
//     }
//     return status;
    }

    void archiveFile(Path fname) { // void ArchiveFile(const std::string& fname) {
//     // Move into another directory.  E.g., for
//     //    dir/foo
//     // rename to
//     //    dir/lost/foo
//     const char* slash = strrchr(fname.c_str(), '/');
//     std::string new_dir;
//     if (slash != NULL) {
//       new_dir.assign(fname.data(), slash - fname.data());
//     }
//     new_dir.append("/lost");
//     env_->CreateDir(new_dir);  // Ignore error
//     std::string new_file = new_dir;
//     new_file.append("/");
//     new_file.append((slash == NULL) ? fname.c_str() : slash + 1);
//     Status s = env_->RenameFile(fname, new_file);
//     Log(options_.info_log, "Archiving %s: %s\n",
//         fname.c_str(), s.ToString().c_str());
    }


// Status RepairDB(const std::string& dbname, const Options& options) {
//   Repairer repairer(dbname, options);
//   return repairer.Run();
// }

}
// // Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// // Use of this source code is governed by a BSD-style license that can be
// // found in the LICENSE file. See the AUTHORS file for names of contributors.
// //
// #include "db/builder.h"
// #include "db/db_impl.h"
// #include "db/dbformat.h"
// #include "db/filename.h"
// #include "db/log_reader.h"
// #include "db/log_writer.h"
// #include "db/memtable.h"
// #include "db/table_cache.h"
// #include "db/version_edit.h"
// #include "db/write_batch_internal.h"
// #include "leveldb/comparator.h"
// #include "leveldb/db.h"
// #include "leveldb/env.h"
// 
// namespace leveldb {
// 
// namespace {
