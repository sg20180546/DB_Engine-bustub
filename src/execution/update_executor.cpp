//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_=exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  txn_=exec_ctx_->GetTransaction();
  if(child_executor_.get()==nullptr) {

  } else {
    assert(child_executor_.get()!=nullptr);
    std::vector<Tuple> child_tp_result_set;
    std::vector<RID> child_rid_result_set;
    size_t size;
    Tuple tuple;
    Tuple prev;
    RID rid;

    child_executor_->Init();
    
    while(child_executor_->Next(&tuple,&rid)) {
      child_rid_result_set.push_back(rid);
      child_tp_result_set.push_back(tuple);
    }
    size=child_rid_result_set.size();
    for(size_t i = 0; i<size;i++) {
      prev=child_tp_result_set[i];
      tuple=GenerateUpdatedTuple(child_tp_result_set[i]);
      table_info_->table_->UpdateTuple(tuple,child_rid_result_set[i],txn_);
      // table_info_->table_->UpdateTuple(tuple,child_rid_result_set[i],txn_)
      for(auto index_info : index_infos_) {
        index_info->index_->DeleteEntry(prev,child_rid_result_set[i],txn_);
        index_info->index_->InsertEntry(tuple,child_rid_result_set[i],txn_);
      }
    }
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { return false; }

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
