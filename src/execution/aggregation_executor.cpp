//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan) , child_(std::move(child)), agg_hash_table_(plan->GetAggregates(),plan_->GetAggregateTypes()), iterator_(agg_hash_table_.Begin()) {}

void AggregationExecutor::Init() {
    key_schema_=plan_->OutputSchema();
    having=plan_->GetHaving();
    child_->Init();

    // if(plan_->GetGroupBys().empty()){

    // }else {
    //     assert(child_!=nullptr);
    //     child_->Init();

    // }
    Tuple tp;
    RID r;
    while(child_->Next(&tp,&r)) {
        auto key=MakeAggregateKey(&tp);
        auto value=MakeAggregateValue(&tp,r);
        value.rid=r;
        agg_hash_table_.InsertCombine(key,value);
    }
    iterator_=agg_hash_table_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    // std::cout<<"next\n";
    
    while(iterator_!=agg_hash_table_.End()){
   

        auto key=iterator_.Key();
        auto value=iterator_.Val();
        std::vector<Value> merged;
        std::vector<Value> result;
        merged.reserve(key_schema_->GetColumnCount());
      
        merged.reserve(key.group_bys_.size()+value.aggregates_.size());
        merged.insert(merged.end(),key.group_bys_.begin(),key.group_bys_.end());
        merged.insert(merged.end(),value.aggregates_.begin(),value.aggregates_.end());

        for(auto column: key_schema_->GetColumns()) {
           auto agg_expr=reinterpret_cast<const AggregateValueExpression*>(column.GetExpr());
           // construct tuple
        //    std::cout<<"construct tuple\n";
           auto val=agg_expr->EvaluateAggregate(key.group_bys_,merged);
        //    std::cout<<"here?\n";
        //    merged.push_back(val);
           result.push_back(val);
        }

        if(having!=nullptr) {
            if(having->EvaluateAggregate(key.group_bys_,value.aggregates_).GetAs<bool>()){
                // std::cout<<"true\n";
                *rid=value.rid;
                // std::cout<<"herel? "<<merged.size()<<"\n";
                assert(key_schema_!=nullptr);
                *tuple=Tuple(result,key_schema_); // <- bug point
                // std::cout<<"next\n";
                iterator_.Next();
                return true;                    
            }
        } else {
            *rid=value.rid;
            *tuple=Tuple(merged,key_schema_);
            iterator_.Next();
            return true;    
        }
        std::cout<<"end\n";

        iterator_.Next();
        // *tuple=Tuple(merged,plan_->OutputSchema());


    }
    return false; 
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
