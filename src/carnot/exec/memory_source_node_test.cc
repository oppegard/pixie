#include <arrow/array.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include "src/common/error.h"

#include "src/carnot/exec/exec_node_mock.h"
#include "src/carnot/exec/memory_source_node.h"
#include "src/carnot/exec/test_utils.h"
#include "src/carnot/proto/test_proto.h"
#include "src/shared/types/arrow_adapter.h"

namespace pl {
namespace carnot {
namespace exec {

using testing::_;

class MemorySourceNodeTest : public ::testing::Test {
 public:
  MemorySourceNodeTest() {
    udf_registry_ = std::make_unique<udf::ScalarUDFRegistry>("test_registry");
    uda_registry_ = std::make_unique<udf::UDARegistry>("test_registry");
    auto table_store = std::make_shared<TableStore>();
    exec_state_ =
        std::make_unique<ExecState>(udf_registry_.get(), uda_registry_.get(), table_store);

    plan::Relation rel =
        plan::Relation({types::DataType::BOOLEAN, types::DataType::TIME64NS}, {"col1", "time_"});

    std::shared_ptr<Table> table = std::make_shared<Table>(rel);
    exec_state_->table_store()->AddTable("cpu", table);

    auto col1 = table->GetColumn(0);
    std::vector<types::BoolValue> col1_in1 = {true, false, true};
    std::vector<types::BoolValue> col1_in2 = {false, false};
    EXPECT_OK(col1->AddBatch(types::ToArrow(col1_in1, arrow::default_memory_pool())));
    EXPECT_OK(col1->AddBatch(types::ToArrow(col1_in2, arrow::default_memory_pool())));

    auto col2 = table->GetColumn(1);
    std::vector<types::Int64Value> col2_in1 = {1, 2, 3};
    std::vector<types::Int64Value> col2_in2 = {5, 6};
    EXPECT_OK(col2->AddBatch(types::ToArrow(col2_in1, arrow::default_memory_pool())));
    EXPECT_OK(col2->AddBatch(types::ToArrow(col2_in2, arrow::default_memory_pool())));
  }

 protected:
  std::unique_ptr<ExecState> exec_state_;
  std::unique_ptr<udf::UDARegistry> uda_registry_;
  std::unique_ptr<udf::ScalarUDFRegistry> udf_registry_;
};

TEST_F(MemorySourceNodeTest, basic) {
  auto op_proto = carnotpb::testutils::CreateTestSource1PB();
  std::unique_ptr<plan::Operator> plan_node = plan::MemorySourceOperator::FromProto(op_proto, 1);
  RowDescriptor output_rd({types::DataType::TIME64NS});

  auto tester = exec::ExecNodeTester<MemorySourceNode, plan::MemorySourceOperator>(
      *plan_node, output_rd, std::vector<RowDescriptor>({}), exec_state_.get());
  EXPECT_TRUE(tester.node()->HasBatchesRemaining());
  tester.GenerateNextResult().ExpectRowBatch(
      RowBatchBuilder(output_rd, 3, false).AddColumn<types::Time64NSValue>({1, 2, 3}).get());
  EXPECT_TRUE(tester.node()->HasBatchesRemaining());
  tester.GenerateNextResult().ExpectRowBatch(
      RowBatchBuilder(output_rd, 2, true).AddColumn<types::Time64NSValue>({5, 6}).get());
  EXPECT_FALSE(tester.node()->HasBatchesRemaining());
  tester.Close();
  EXPECT_EQ(5, tester.node()->RowsProcessed());
  EXPECT_EQ(sizeof(int64_t) * 5, tester.node()->BytesProcessed());
}

TEST_F(MemorySourceNodeTest, range) {
  auto op_proto = carnotpb::testutils::CreateTestSourceRangePB();
  std::unique_ptr<plan::Operator> plan_node = plan::MemorySourceOperator::FromProto(op_proto, 1);
  RowDescriptor output_rd({types::DataType::TIME64NS});

  auto tester = exec::ExecNodeTester<MemorySourceNode, plan::MemorySourceOperator>(
      *plan_node, output_rd, std::vector<RowDescriptor>({}), exec_state_.get());
  EXPECT_TRUE(tester.node()->HasBatchesRemaining());

  tester.GenerateNextResult().ExpectRowBatch(
      RowBatchBuilder(output_rd, 1, false).AddColumn<types::Time64NSValue>({3}).get());
  EXPECT_TRUE(tester.node()->HasBatchesRemaining());
  tester.GenerateNextResult().ExpectRowBatch(
      RowBatchBuilder(output_rd, 1, true).AddColumn<types::Time64NSValue>({5}).get());
  EXPECT_FALSE(tester.node()->HasBatchesRemaining());
  tester.Close();
}

TEST_F(MemorySourceNodeTest, empty_range) {
  auto op_proto = carnotpb::testutils::CreateTestSourceEmptyRangePB();
  std::unique_ptr<plan::Operator> plan_node = plan::MemorySourceOperator::FromProto(op_proto, 1);
  RowDescriptor output_rd({types::DataType::TIME64NS});

  auto tester = exec::ExecNodeTester<MemorySourceNode, plan::MemorySourceOperator>(
      *plan_node, output_rd, std::vector<RowDescriptor>({}), exec_state_.get());
  EXPECT_FALSE(tester.node()->HasBatchesRemaining());
  tester.Close();
}

TEST_F(MemorySourceNodeTest, all_range) {
  auto op_proto = carnotpb::testutils::CreateTestSourceAllRangePB();
  std::unique_ptr<plan::Operator> plan_node = plan::MemorySourceOperator::FromProto(op_proto, 1);
  RowDescriptor output_rd({types::DataType::TIME64NS});

  auto tester = exec::ExecNodeTester<MemorySourceNode, plan::MemorySourceOperator>(
      *plan_node, output_rd, std::vector<RowDescriptor>({}), exec_state_.get());
  EXPECT_TRUE(tester.node()->HasBatchesRemaining());

  tester.GenerateNextResult().ExpectRowBatch(
      RowBatchBuilder(output_rd, 1, false).AddColumn<types::Time64NSValue>({3}).get());
  EXPECT_TRUE(tester.node()->HasBatchesRemaining());
  tester.GenerateNextResult().ExpectRowBatch(
      RowBatchBuilder(output_rd, 2, true).AddColumn<types::Time64NSValue>({5, 6}).get());
  EXPECT_FALSE(tester.node()->HasBatchesRemaining());
  tester.Close();
}

}  // namespace exec
}  // namespace carnot
}  // namespace pl
