//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) , plan_(plan), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
    left_executor_->Init();
    
    right_executor_->Init();
    predicate_=plan_->Predicate();
    left_schema_=plan_->GetLeftPlan()->OutputSchema();
    right_schema_=plan_->GetRightPlan()->OutputSchema();
    key_schema_=plan_->OutputSchema();


    left_executor_->Next(&l_tp,&l_rid);
    // Tuple tp;
    // RID rid;
    // while(left_executor_->Next(&tp,&rid)) {
    //     left_result_.push_back(tp);
    // }
    // while(right_executor_->Next(&tp,&rid)) {
    //     right_result.push_back(tp);
    // }
    // ls=left_result_.size();
    // rs=right_result.size();
    // std::cout<<ls<<" "<<rs<<"\n";
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    // std::vector<Value> values;
    // while(i<ls) {
    //     while(j<rs) {
    //         // std::cout<<i<<" "<<j<<"\n";
    //         if(predicate_->EvaluateJoin(&left_result_[i],left_schema_,&right_result[j],right_schema_).GetAs<bool>()) {
    //             // how to merge ?'
    //             // for(auto& column:key_schema_->GetColumns()) {
                    
    //             // }
    //             *tuple=left_result_[i];
    //             j++;
    //             return true;
    //         }
    //         j++;
    //     }
    //     j=0;
    //     i++;
    // }

    
    while(true) {
        // std::cout<<"hello\n";
        while(right_executor_->Next(&r_tp,&r_rid)) {
            // std::cout<<"right exec?\n";
            if(predicate_->EvaluateJoin(&l_tp,left_schema_,&r_tp,right_schema_).GetAs<bool>()) {
                // *tuple=l_tp;
                std::vector<Value> values;
                for(auto& column : key_schema_->GetColumns()) {
                    // auto column_expr= column.GetExpr();
                    auto column_expr = reinterpret_cast<const ColumnValueExpression *>(column.GetExpr());
                    if(column_expr->GetTupleIdx()==0){
                        values.push_back(l_tp.GetValue(left_schema_,column_expr->GetColIdx())); 
                    }else {
                        values.push_back(r_tp.GetValue(right_schema_,column_expr->GetColIdx()));
                    }
                }
                *tuple=Tuple(values,key_schema_);
                return true;
            }

        }
        right_executor_->Init();
        if(left_executor_->Next(&l_tp,&l_rid)==false) {
            break;
        }
        // std::cout<<"here?\n";
        
        // std::cout<<"here1?\n";
    }
    return false; 
}


}  // namespace bustub
