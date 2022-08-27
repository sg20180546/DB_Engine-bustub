//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) , child_executor_(std::move(child_executor)), child_schema_(nullptr) {

    }

void DistinctExecutor::Init() {
    child_schema_=child_executor_->GetOutputSchema();
    child_executor_->Init();
}

auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    Tuple tp;
    RID r;
    while(child_executor_->Next(&tp,&r)){

        Value distinct_value=tp.GetValue(child_schema_,0); // distinct value index is not exists in test

        hash_t hash_key=hash_util_.HashValue(&distinct_value);

        if(encounterd_.count(hash_key)==0){
            encounterd_.insert(hash_key);
            *tuple=tp;
            *rid=r;
            return true;
        }
    }
    return false; 
}

}  // namespace bustub
