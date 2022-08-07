//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_child)), right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
    // insert left_child
    hash_map_.clear();
    left_join_column_expr_=reinterpret_cast<const ColumnValueExpression*>(plan_->LeftJoinKeyExpression());
    right_join_column_expr_=reinterpret_cast<const ColumnValueExpression*>(plan_->RightJoinKeyExpression());
    
    left_schema_=plan_->GetLeftPlan()->OutputSchema();
    right_schema_=plan_->GetRightPlan()->OutputSchema();
    left_executor_->Init();
    key_schema_=plan_->OutputSchema();
    Tuple tp;
    RID r;
    while(left_executor_->Next(&tp,&r)) {
        // insert 
        Value join_value=tp.GetValue(left_schema_,left_join_column_expr_->GetColIdx());
        hash_t hash_key=hash_util_.HashValue(&join_value);

        // if there is overlapped left hash key ?
        hash_map_.insert({hash_key,{r,tp}});
        
    }

    right_executor_->Init();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    Tuple tp;
    RID r;

    while(right_executor_->Next(&tp,&r)) {
        // find in hash table
        Value join_value=tp.GetValue(right_schema_,right_join_column_expr_->GetColIdx());
        hash_t hash_key=hash_util_.HashValue(&join_value);
        auto left_child=hash_map_.find(hash_key);
        if(left_child!=hash_map_.end()){
            *rid=left_child->second.first;
            *tuple=MergeTuple(left_child->second.second,tp);
            return true;
        }
    }
    return false; 
}

auto HashJoinExecutor::MergeTuple(Tuple& left, Tuple& right) -> Tuple {
    std::vector<Value> values;
    for(auto& column: plan_->OutputSchema()->GetColumns()) {
        auto column_expr=reinterpret_cast<const ColumnValueExpression*>(column.GetExpr());
        if(column_expr->GetTupleIdx()==0) {
            values.push_back(left.GetValue(left_schema_,column_expr->GetColIdx()));
        }else {
            values.push_back(right.GetValue(right_schema_,column_expr->GetColIdx()));
        }
    }
    return Tuple(values,key_schema_);
}

}  // namespace bustub
