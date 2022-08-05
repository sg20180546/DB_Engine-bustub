//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){}

void DeleteExecutor::Init() {
    table_info_=exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
    index_infos_=exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    txn_=exec_ctx_->GetTransaction();
    if(child_executor_.get()==nullptr) {

    }else {
        assert(child_executor_.get()!=nullptr);
        std::vector<Tuple> child_tp_result_set;
        std::vector<RID> child_rid_result_set;
        size_t size;
        RID rid;
        Tuple tuple;

        child_executor_->Init();
        while(child_executor_->Next(&tuple,&rid)) {
            child_rid_result_set.push_back(rid);
            child_tp_result_set.push_back(tuple);
        }

        size=child_rid_result_set.size();
        // std::cout<<"size : "<<size<<"\n";
        for(size_t i = 0 ; i<size ; i++) {
            // std::cout<<child_rid_result_set[i].ToString();
            table_info_->table_->ApplyDelete(child_rid_result_set[i],txn_);
            for(auto index_info : index_infos_) {
                index_info->index_->DeleteEntry(child_tp_result_set[i],child_rid_result_set[i],txn_);
            }
        }

    }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { return false; }

}  // namespace bustub
