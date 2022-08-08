//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), limit_(0),  cur_(0), key_schema_(nullptr), child_schema_(nullptr) {}

void LimitExecutor::Init() {

    child_executor_->Init();
    key_schema_=GetOutputSchema();
    size_t column_count=key_schema_->GetColumnCount();
    std::string column_name;
    uint32_t column_idx;
    auto table_info=exec_ctx_->GetCatalog()->GetTable(reinterpret_cast<const SeqScanPlanNode*>(plan_->GetChildPlan())->GetTableOid());
    // child_schema_=child_executor_->GetOutputSchema();

    for(uint32_t i=0;i<column_count;i++){
        column_name.assign(plan_->OutputSchema()->GetColumn(i).GetName());
        column_idx=table_info->schema_.GetColIdx(column_name);
        // if not found, exception handling
        key_attrs_.push_back(column_idx);
    }
    limit_=plan_->GetLimit();
    
    child_schema_=child_executor_->GetOutputSchema();
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    Tuple tp;
    RID r;

    while(child_executor_->Next(&tp,&r)&&cur_<limit_){
        *tuple=tp.KeyFromTuple(*child_schema_,*key_schema_,key_attrs_);
        *rid=r;
        cur_++;
        return true;
    }
    return false; 
}

}  // namespace bustub
