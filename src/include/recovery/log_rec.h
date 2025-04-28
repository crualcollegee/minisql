#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 */
struct LogRec {
    LogRec() = default;

    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};
    txn_id_t txn;
    KeyType old_key;
    KeyType new_key;
    ValType old_val;
    ValType new_val;
    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
    LogRec log;
    log.type_ = LogRecType::kInsert;
    log.txn = txn_id;
    
    log.prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
    log.lsn_ = LogRec::next_lsn_++;
    log.new_key = ins_key;
    log.new_val = ins_val;
    return std::make_shared<LogRec>(log);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
    LogRec log;
    log.type_ = LogRecType::kDelete;
    log.txn = txn_id;
    
    log.prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
    log.lsn_ = LogRec::next_lsn_++;
    log.new_key = del_key;
    log.new_val = del_val;
    return std::make_shared<LogRec>(log);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
    LogRec log;
    log.type_ = LogRecType::kUpdate;
    log.txn = txn_id;
    
    log.prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
    log.lsn_ = LogRec::next_lsn_++;
    log.old_key = old_key;
    log.old_val = old_val;
    log.new_key = new_key;
    log.new_val = new_val;
    return std::make_shared<LogRec>(log);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
    LogRec log;
    log.type_ = LogRecType::kBegin;
    log.txn = txn_id;
    
    log.prev_lsn_ = INVALID_LSN;
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
    log.lsn_ = LogRec::next_lsn_++;
    return std::make_shared<LogRec>(log);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
    LogRec log;
    log.type_ = LogRecType::kCommit;
    log.txn = txn_id;
    
    log.prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
    log.lsn_ = LogRec::next_lsn_++;
    return std::make_shared<LogRec>(log);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
    LogRec log;
    log.type_ = LogRecType::kAbort;
    log.txn = txn_id;
    
    log.prev_lsn_ = LogRec::prev_lsn_map_[txn_id];
    LogRec::prev_lsn_map_[txn_id] = LogRec::next_lsn_;
    log.lsn_ = LogRec::next_lsn_++;
    return std::make_shared<LogRec>(log);
}

#endif  // MINISQL_LOG_REC_H
