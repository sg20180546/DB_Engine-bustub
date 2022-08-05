//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) 
                                : AbstractExecutor(exec_ctx), plan_(plan), iterator_(nullptr,RID(),nullptr) {}

void SeqScanExecutor::Init() {
    key_attrs_.clear();
    
    uint32_t column_idx;
    std::string column_name;
    uint32_t column_count=plan_->OutputSchema()->GetColumnCount();    
    table_info_=exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    iterator_=table_info_->table_.get()->Begin(exec_ctx_->GetTransaction());
  
    for(uint32_t i=0;i<column_count;i++){
        column_name.assign(plan_->OutputSchema()->GetColumn(i).GetName());
        column_idx=table_info_->schema_.GetColIdx(column_name);
        // if not found, exception handling
        key_attrs_.push_back(column_idx);
    }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {

    const AbstractExpression* predicate=plan_->GetPredicate();
    Schema schema=table_info_->schema_;
    const Schema* key_schema=plan_->OutputSchema();
    TableIterator end =table_info_->table_.get()->End();

    if(iterator_==end) {
        return false;
    }
    
    if(predicate!=nullptr) {
        for(;iterator_!=end;iterator_++) {
            Tuple tp= iterator_->KeyFromTuple(table_info_->schema_,*key_schema,key_attrs_);
            if(predicate->Evaluate(&tp,&schema).GetAs<bool>()) {
                *tuple=tp;
                *rid=iterator_->GetRid();
                iterator_++;
                return true;
            }
            // iterator_++;
        }
        return false;
    } else {
        Tuple tp=iterator_->KeyFromTuple(table_info_->schema_,*key_schema,key_attrs_);
        *rid=iterator_->GetRid();
        *tuple=tp;
        iterator_++;
        return true;
    }
}

}  // namespace bustub
