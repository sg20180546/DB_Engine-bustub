//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
    
    table_info_=exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    [[maybe_unused]] RID rid;

    if(plan_->IsRawInsert()) {
        
        assert(child_executor_.get()==nullptr);
        size_t insert_size=plan_->RawValues().size();
        for(size_t i=0;i<insert_size;i++) {
            Tuple tp(plan_->RawValuesAt(i),&table_info_->schema_);
            
            table_info_->table_->InsertTuple(tp,&rid,exec_ctx_->GetTransaction());
        }
    } else {
        assert(child_executor_.get()!=nullptr);
        std::vector<Tuple> child_result_set;
        // std::cout<<child_executor_->GetName()<<"\n";
        child_executor_->Init();

        Tuple tuple;
        RID rid;
        while (child_executor_->Next(&tuple, &rid)) {
            child_result_set.push_back(tuple);
        }

        for(auto tp : child_result_set) {
            table_info_->table_->InsertTuple(tp,&rid,exec_ctx_->GetTransaction());
        }
    }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { return false; }

}  // namespace bustub
