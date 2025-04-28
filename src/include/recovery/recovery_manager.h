#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
    lsn_t checkpoint_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase persist_data_{};

    inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

    inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
    /**
    * TODO: Student Implement
    */
    void Init(CheckPoint &last_checkpoint) {
        persist_lsn_ = last_checkpoint.checkpoint_lsn_;
        active_txns_ = last_checkpoint.active_txns_;
        data_ = last_checkpoint.persist_data_;
    }

    /**
    * TODO: Student Implement
    */
    void RedoPhase() {
        for(int i = persist_lsn_;i < LogRec::next_lsn_;i++){
            auto log = log_recs_[i];
            active_txns_[log->txn] = log->lsn_;
            switch (log->type_) {
                case LogRecType::kInsert:
                    data_[log->new_key] = log->new_val;
                    break;
                case LogRecType::kDelete:
                    data_.erase(log->new_key);
                    break;
                case LogRecType::kUpdate:
                    data_.erase(log->old_key);
                    data_[log->new_key] = log->new_val;
                    break;
                case LogRecType::kBegin:
                    active_txns_[log->txn] = log->lsn_;
                    break;
                case LogRecType::kCommit:
                    active_txns_.erase(log->lsn_);
                    break;
                
                case LogRecType::kAbort:{
                    auto it = log;
                    while(it->prev_lsn_ != INVALID_LSN){
                        it = log_recs_[it->prev_lsn_];
                        if(it->type_ == LogRecType::kInsert){
                            data_.erase(it->new_key);
                        }else if(it->type_ == LogRecType::kUpdate){
                            data_.erase(it->new_key);
                            data_[it->old_key] = it->old_val;
                        }
                        else if(it->type_ == LogRecType::kDelete){
                            data_[it->new_key] = it->new_val;
                        }
                    }
                    break;
                }
                default: break;    
            }
            
        } 
    }

    void UndoPhase() {
        for(auto it : active_txns_){
            auto log = log_recs_.find(it.second);
            while(log != log_recs_.end()){
                auto it = log->second;
                switch(it->type_){
                    case LogRecType::kInsert:
                        data_.erase(it->new_key);
                        break;
                    case LogRecType::kUpdate:
                        data_.erase(it->new_key);
                        data_[it->old_key] = it->old_val;
                        break;
                    case LogRecType::kDelete:
                        data_[it->old_key] = it->old_val;
                        break;
                        
                    default: break;
                }
                log = log_recs_.find(log->second->prev_lsn_);
            }
        }
    }

    // used for test only
    void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

    // used for test only
    inline KvDatabase &GetDatabase() { return data_; }

private:
    std::map<lsn_t, LogRecPtr> log_recs_{};
    lsn_t persist_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
