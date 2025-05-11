#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * TODO: Student Implement
 */
bool LockManager::LockShared(Txn *txn, const RowId &rid) {
    if(txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
        txn->SetState(TxnState::kAborted);
        return false;
    }
    if(txn->GetState()!= TxnState::kGrowing) {
        txn->SetState(TxnState::kAborted);
        return false;
    }
    auto &lock_req_queue = lock_table_[rid];
    lock_req_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kShared);
    std::unique_lock<std::mutex> lock(latch_);
    lock_req_queue.cv_.wait(lock, [&] {
        if (lock_req_queue.is_writing_ || lock_req_queue.is_upgrading_) {
            return false;
        }
        return true;
    });
    lock_req_queue.GetLockRequestIter(txn->GetTxnId())->granted_ = LockMode::kShared;
    lock_req_queue.sharing_cnt_++;
    lock_req_queue.cv_.notify_all();
    txn->GetSharedLockSet().emplace(rid);
    return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
    if(txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
        txn->SetState(TxnState::kAborted);
        return false;
    }
    if(txn->GetState()!= TxnState::kGrowing) {
        txn->SetState(TxnState::kAborted);
        return false;
    }
    auto &lock_req_queue = lock_table_[rid];
    //auto lock_request = lock_req_queue.GetLockRequestIter(txn->GetTxnId());
    lock_req_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kExclusive);
    std::unique_lock<std::mutex> lock(latch_);
    lock_req_queue.cv_.wait(lock, [&] {
        if (lock_req_queue.is_writing_ || lock_req_queue.is_upgrading_ || lock_req_queue.sharing_cnt_ > 0) {
            return false;
        }
        return true;
    });
    lock_req_queue.GetLockRequestIter(txn->GetTxnId())->granted_ = LockMode::kExclusive;
    lock_req_queue.is_writing_ = true;
    lock_req_queue.cv_.notify_all();
    txn->GetExclusiveLockSet().emplace(rid);
    return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
    if(txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
        txn->SetState(TxnState::kAborted);
        return false;
    }
    
    if(txn->GetState()!= TxnState::kGrowing) {
        txn->SetState(TxnState::kAborted);
        return false;
    }
    auto &lock_req_queue = lock_table_[rid];
    auto lock_request = lock_req_queue.GetLockRequestIter(txn->GetTxnId());
    if(lock_request == lock_req_queue.req_list_.end()) {
        return false;
    }
    if(lock_request->granted_ != LockMode::kShared) {
        return false;
    }
    if (lock_req_queue.is_upgrading_) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kUpgradeConflict);
      }
    if(lock_request == lock_req_queue.req_list_.end()) {
        return false;
    }
    lock_request->lock_mode_ = LockMode::kExclusive;
    lock_req_queue.is_upgrading_ = true;
    std::unique_lock<std::mutex> lock(latch_);
    lock_req_queue.cv_.wait(lock, [&] {
        if (lock_req_queue.is_writing_ || lock_req_queue.sharing_cnt_ > 1) {
            return false;
        }
        return true;
    });
    if (txn->GetState() != TxnState::kGrowing) {
        lock_req_queue.is_upgrading_ = false;
        lock_req_queue.cv_.notify_all();
        return false;
      }
    lock_request->granted_ = LockMode::kExclusive;
    lock_req_queue.sharing_cnt_--;
    lock_req_queue.is_writing_ = true;
    lock_req_queue.is_upgrading_ = false;
    txn->GetSharedLockSet().erase(rid);
    txn->GetExclusiveLockSet().emplace(rid);
    lock_req_queue.cv_.notify_all();
    
    return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::Unlock(Txn *txn, const RowId &rid) {
    auto &lock_req_queue = lock_table_[rid];
    auto lock_request = lock_req_queue.GetLockRequestIter(txn->GetTxnId());
    if (lock_request == lock_req_queue.req_list_.end()) {
        return false;
    }
    if(lock_request->granted_ == LockMode::kShared) {
        lock_req_queue.sharing_cnt_--;
    }
    if(lock_request->granted_ == LockMode::kExclusive) {
        lock_req_queue.is_writing_ = false;
    }
    auto erased = lock_req_queue.EraseLockRequest(txn->GetTxnId());
    if(!erased) {
        return false;
    }
    lock_req_queue.cv_.notify_all();
    if(txn -> GetState() == TxnState::kGrowing)txn -> SetState(TxnState::kShrinking);//若当前事务处于Growing状态，则将其设置为Shrinking状态
    txn -> GetExclusiveLockSet().erase(rid);
    txn -> GetSharedLockSet().erase(rid);
    return true;
}

/**
 * TODO: Student Implement
 */
void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
    // 获取锁请求队列
    auto &lock_req_queue = lock_table_[rid];

    // 检查事务的状态是否允许获取锁
    if (txn->GetState() != TxnState::kGrowing) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
    }

    // 检查事务的隔离级别是否允许获取锁
    if (txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
        txn->SetState(TxnState::kAborted);
        throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockSharedOnReadUncommitted);
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
    // 遍历锁请求队列中的所有请求
    for (const auto &lock_request : req_queue.req_list_) {
        // 如果当前事务的状态已经是 Aborted，则直接返回
        if (txn->GetState() == TxnState::kAborted) {
            return;
        }

        // 检查是否存在冲突的锁请求
        if (lock_request.txn_id_ != txn->GetTxnId() && lock_request.granted_ == LockMode::kExclusive) {
            txn->SetState(TxnState::kAborted);
            throw TxnAbortException(txn->GetTxnId(), AbortReason::kDeadlock);
        }
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
    waits_for_[t1].insert(t2);
}

/**
 * TODO: Student Implement
 */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
    waits_for_[t1].erase(t2);
}

/**
 * TODO: Student Implement
 */
bool LockManager::HasCycle(txn_id_t &txn_id) {
    std::unordered_set<txn_id_t> visited;       // 已访问的节点集合
    std::unordered_set<txn_id_t> rec_stack;     // 当前递归路径上的节点集合

    // 遍历所有事务，按事务 ID 从小到大进行 DFS
    for (const auto &pair : waits_for_) {
        txn_id_t start_node = pair.first;

        // 如果节点未被访问，则从该节点开始 DFS
        if (visited.find(start_node) == visited.end()) {
            if (DFS(start_node, visited, rec_stack, txn_id)) {
                return true; // 找到循环
            }
        }
    }

    return false; // 如果没有检测到循环，返回 false
}

bool LockManager::DFS(txn_id_t current, std::unordered_set<txn_id_t> &visited,
                      std::unordered_set<txn_id_t> &rec_stack, txn_id_t &txn_id) {
    visited.insert(current);       // 标记当前节点为已访问
    rec_stack.insert(current);     // 将当前节点加入递归路径

    // 遍历当前节点的邻居，按事务 ID 从小到大排序
    std::vector<txn_id_t> neighbors(waits_for_[current].begin(), waits_for_[current].end());
    std::sort(neighbors.begin(), neighbors.end());

    for (const auto &neighbor : neighbors) {
        if (rec_stack.find(neighbor) != rec_stack.end()) {
            // 如果邻居在递归路径中，说明存在循环
            txn_id = current; 
            return true;
        }

        if (visited.find(neighbor) == visited.end()) {
            // 如果邻居未被访问，递归调用 DFS
            if (DFS(neighbor, visited, rec_stack, txn_id)) {
                
                return true;
            }
        }
    }

    rec_stack.erase(current); // 当前节点的所有邻居都已处理，移出递归路径
    return false;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
    waits_for_.erase(txn_id);

    auto *txn = txn_mgr_->GetTransaction(txn_id);

    for (const auto &row_id: txn->GetSharedLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }

    for (const auto &row_id: txn->GetExclusiveLockSet()) {
        for (const auto &lock_req: lock_table_[row_id].req_list_) {
            if (lock_req.granted_ == LockMode::kNone) {
                RemoveEdge(lock_req.txn_id_, txn_id);
            }
        }
    }
}

/**
 * TODO: Student Implement
 */
void LockManager::RunCycleDetection() {
    while (enable_cycle_detection_) {
        // 清空等待图
        waits_for_.clear();

        // 遍历锁表，构建等待图
        for (const auto &lock_entry : lock_table_) {
            auto &request_list = lock_entry.second.req_list_;
            for (auto outer_iter = request_list.rbegin(); outer_iter != request_list.rend(); ++outer_iter) {
                if (outer_iter->granted_ == LockMode::kNone) {
                    break;
                }
                for (auto inner_iter = outer_iter; inner_iter != request_list.rend(); ++inner_iter) {
                    if (inner_iter->granted_ == LockMode::kNone) {
                        bool is_conflict = (outer_iter->lock_mode_ == LockMode::kShared && inner_iter->lock_mode_ == LockMode::kExclusive) ||
                                           (outer_iter->lock_mode_ == LockMode::kExclusive);
                        if (is_conflict) {
                            AddEdge(inner_iter->txn_id_, outer_iter->txn_id_);
                        }
                    }
                }
            }
        }

        // 检测循环并中止事务
        txn_id_t youngest_txn_in_cycle;
        if (HasCycle(youngest_txn_in_cycle)) {
            txn_mgr_->Abort(txn_mgr_->GetTransaction(youngest_txn_in_cycle));
        }

        // 等待下一个检测周期
        std::this_thread::sleep_for(cycle_detection_interval_);
    }
}

/**
 * TODO: Student Implement
 */
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
    std::vector<std::pair<txn_id_t, txn_id_t>> result;
    for (const auto &pair : waits_for_) {
        txn_id_t t1 = pair.first;
        for (const auto &t2 : pair.second) {
            result.emplace_back(t1, t2);
        }
    }
    return result;
}
