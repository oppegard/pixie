#include "src/carnot/compiler/ast_visitor.h"

#include "src/carnot/compiler/compiler_error_context/compiler_error_context.h"
#include "src/carnot/compiler/ir/pattern_match.h"
#include "src/carnot/compiler/objects/none_object.h"

namespace pl {
namespace carnot {
namespace compiler {
using pypa::AstType;

StatusOr<FuncIR::Op> ASTVisitorImpl::GetOp(const std::string& python_op, const pypa::AstPtr node) {
  auto op_find = FuncIR::op_map.find(python_op);
  if (op_find == FuncIR::op_map.end()) {
    return CreateAstError(node, "Operator '$0' not handled", python_op);
  }
  return op_find->second;
}

ASTVisitorImpl::ASTVisitorImpl(IR* ir_graph, CompilerState* compiler_state)
    : ir_graph_(ir_graph), compiler_state_(compiler_state) {
  var_table_ = VarTable();
  var_table_[kDataframeOpId] = std::shared_ptr<FuncObject>(
      new FuncObject(kDataframeOpId, {"table", "select"}, {{"select", "[]"}}, /*has_kwargs*/ false,
                     std::bind(&ASTVisitorImpl::ProcessDataframeOp, this, std::placeholders::_1,
                               std::placeholders::_2)));
  // TODO(philkuz) (PL-1038) figure out naming for Print syntax to get around parser.
  // var_table_[kPrintOpId] = std::shared_ptr<FuncObject>(new FuncObject(
  //     kPrintOpId, {"out", "name", "cols"}, {{"name", ""}, {"cols", "[]"}}, /*has_kwargs*/ false,
  //     std::bind(&ASTVisitorImpl::ProcessPrint, this, std::placeholders::_1,
  //               std::placeholders::_2)));
}

StatusOr<QLObjectPtr> ASTVisitorImpl::ProcessDataframeOp(const pypa::AstPtr& ast,
                                                         const ParsedArgs& args) {
  IRNode* table = args.GetArg("table");
  IRNode* select = args.GetArg("select");
  if (!Match(table, String())) {
    return table->CreateIRNodeError("'table' must be a string, got $0", table->type_string());
  }

  if (!Match(select, ListWithChildren(String()))) {
    return select->CreateIRNodeError("'select' must be a list of strings.");
  }

  std::string table_name = static_cast<StringIR*>(table)->str();
  PL_ASSIGN_OR_RETURN(std::vector<std::string> columns,
                      ParseStringListIR(static_cast<ListIR*>(select)));
  PL_ASSIGN_OR_RETURN(MemorySourceIR * mem_source_op, ir_graph_->MakeNode<MemorySourceIR>(ast));
  PL_RETURN_IF_ERROR(mem_source_op->Init(table_name, columns));
  return StatusOr(std::make_shared<Dataframe>(mem_source_op));
}

StatusOr<QLObjectPtr> ASTVisitorImpl::ProcessPrint(const pypa::AstPtr& ast,
                                                   const ParsedArgs& args) {
  IRNode* out = args.GetArg("out");
  IRNode* name = args.GetArg("name");
  IRNode* cols = args.GetArg("cols");

  if (!Match(out, Operator())) {
    return out->CreateIRNodeError("'out' must be a dataframe", out->type_string());
  }

  if (!Match(name, String())) {
    return name->CreateIRNodeError("'name' must be a string");
  }

  if (!Match(cols, ListWithChildren(String()))) {
    return cols->CreateIRNodeError("'cols' must be a list of strings.");
  }

  OperatorIR* out_op = static_cast<OperatorIR*>(out);
  std::string out_name = static_cast<StringIR*>(name)->str();
  PL_ASSIGN_OR_RETURN(std::vector<std::string> columns,
                      ParseStringListIR(static_cast<ListIR*>(cols)));

  PL_ASSIGN_OR_RETURN(MemorySinkIR * mem_sink_op, ir_graph_->MakeNode<MemorySinkIR>(ast));
  PL_RETURN_IF_ERROR(mem_sink_op->Init(out_op, out_name, columns));
  return StatusOr(std::make_shared<NoneObject>(mem_sink_op));
}

Status ASTVisitorImpl::ProcessExprStmtNode(const pypa::AstExpressionStatementPtr& e) {
  switch (e->expr->type) {
    case AstType::Call:
      return ProcessOpCallNode(PYPA_PTR_CAST(Call, e->expr)).status();
    default:
      return CreateAstError(e, "Expression node not defined");
  }
}

StatusOr<IRNode*> ASTVisitorImpl::ProcessSingleExpressionModule(const pypa::AstModulePtr& module) {
  OperatorContext op_context({}, "");
  const std::vector<pypa::AstStmt>& items_list = module->body->items;
  if (items_list.size() != 1) {
    return CreateAstError(module,
                          "ProcessModuleExpression only works for single lined statements.");
  }
  const pypa::AstStmt& stmt = items_list[0];
  switch (stmt->type) {
    case pypa::AstType::ExpressionStatement: {
      return ProcessData(PYPA_PTR_CAST(ExpressionStatement, stmt)->expr, op_context);
      break;
    }
    default: {
      return CreateAstError(module, "Want expression, got $0", GetAstTypeName(stmt->type));
    }
  }
}

Status ASTVisitorImpl::ProcessModuleNode(const pypa::AstModulePtr& m) {
  pypa::AstStmtList items_list = m->body->items;
  // iterate through all the items on this list.
  std::vector<Status> status_vector;
  for (pypa::AstStmt stmt : items_list) {
    switch (stmt->type) {
      case pypa::AstType::ExpressionStatement: {
        Status result = ProcessExprStmtNode(PYPA_PTR_CAST(ExpressionStatement, stmt));
        if (!result.ok()) {
          status_vector.push_back(result);
        }
        break;
      }
      case pypa::AstType::Assign: {
        Status result = ProcessAssignNode(PYPA_PTR_CAST(Assign, stmt));
        if (!result.ok()) {
          status_vector.push_back(result);
        }
        break;
      }
      default: {
        status_vector.push_back(
            CreateAstError(m, "Can't parse expression of type $0", GetAstTypeName(stmt->type)));
      }
    }
  }
  return MergeStatuses(status_vector);
}

Status ASTVisitorImpl::ProcessSubscriptMapAssignment(const pypa::AstSubscriptPtr& subscript,
                                                     const pypa::AstPtr& expr_node) {
  if (subscript->value->type != AstType::Name) {
    return CreateAstError(expr_node, "Subscript must be on a Name node, received: $0",
                          GetAstTypeName(subscript->value->type));
  }
  auto assign_name = PYPA_PTR_CAST(Name, subscript->value);
  auto assign_name_string = GetNameAsString(subscript->value);

  // Check to make sure this dataframe exists
  PL_ASSIGN_OR_RETURN(auto parent_op, LookupName(assign_name));

  if (subscript->slice->type != AstType::Index) {
    return CreateAstError(subscript->slice,
                          "Expected to receive index as subscript slice, received $0.",
                          GetAstTypeName(subscript->slice->type));
  }
  auto index = PYPA_PTR_CAST(Index, subscript->slice);
  if (index->value->type != AstType::Str) {
    return CreateAstError(index->value,
                          "Expected to receive string as subscript index value, received $0.",
                          GetAstTypeName(index->value->type));
  }
  auto col_name = PYPA_PTR_CAST(Str, index->value)->value;

  // Maps can only assign to the same table as the input table when of the form:
  // df['foo'] = df['bar'] + 2
  OperatorContext op_context{{parent_op}, kMapOpId, {assign_name_string}};
  PL_ASSIGN_OR_RETURN(auto result, ProcessData(expr_node, op_context));
  if (!result->IsExpression()) {
    return CreateAstError(
        expr_node, "Expected to receive expression as map subscript assignment value, received $0.",
        GetAstTypeName(index->value->type));
  }
  auto expr = static_cast<ExpressionIR*>(result);

  // TODO(nserrino): Remove this conversion into lambdas once lambdas are fully removed for maps.
  PL_ASSIGN_OR_RETURN(LambdaIR * map_lambda_ir_node, ir_graph_->MakeNode<LambdaIR>());
  // Pull in all columns needed in fn.
  ColExpressionVector map_exprs{ColumnExpression{col_name, expr}};
  PL_RETURN_IF_ERROR(
      map_lambda_ir_node->Init(std::unordered_set<std::string>({col_name}), map_exprs, expr_node));

  PL_ASSIGN_OR_RETURN(MapIR * ir_node, ir_graph_->MakeNode<MapIR>());
  ArgMap map_args{{{"fn", map_lambda_ir_node}}, {}};
  PL_RETURN_IF_ERROR(ir_node->Init(parent_op, map_args, expr_node));
  ir_node->set_keep_input_columns(true);

  var_table_[assign_name_string] = std::make_shared<Dataframe>(ir_node);

  return Status::OK();
}

Status ASTVisitorImpl::ProcessAssignNode(const pypa::AstAssignPtr& node) {
  // Check # nodes to assign.
  if (node->targets.size() != 1) {
    return CreateAstError(node, "We only support single target assignment.");
  }
  // Get the name that we are targeting.
  auto target_node = node->targets[0];

  // Special handler for this type of map statement: df['foo'] = df['bar']
  if (target_node->type == AstType::Subscript) {
    return ProcessSubscriptMapAssignment(PYPA_PTR_CAST(Subscript, node->targets[0]), node->value);
  }

  if (target_node->type != AstType::Name) {
    return CreateAstError(target_node, "Assignment target must be a Name");
  }

  std::string assign_name = GetNameAsString(target_node);
  // Get the object that we want to assign.
  switch (node->value->type) {
    case AstType::Call: {
      PL_ASSIGN_OR_RETURN(var_table_[assign_name],
                          ProcessOpCallNode(PYPA_PTR_CAST(Call, node->value)));
      break;
    }
    default: {
      return CreateAstError(node->value, "Assign value must be a function call.");
    }
  }
  return Status::OK();
}  // namespace compiler

StatusOr<std::string> ASTVisitorImpl::GetFuncName(const pypa::AstCallPtr& node) {
  std::string func_name;
  switch (node->function->type) {
    case AstType::Name: {
      func_name = GetNameAsString(node->function);
      break;
    }
    case AstType::Attribute: {
      auto attr = PYPA_PTR_CAST(Attribute, node->function);
      if (attr->attribute->type != AstType::Name) {
        return CreateAstError(node->function, "Couldn't get string name out of node of type $0.",
                              GetAstTypeName(attr->attribute->type));
      }
      func_name = GetNameAsString(attr->attribute);
      break;
    }
    default: {
      return CreateAstError(node->function, "Couldn't get string name out of node of type $0.",
                            GetAstTypeName(node->function->type));
    }
  }
  return func_name;
}

StatusOr<ArgMap> ASTVisitorImpl::ProcessArgs(const pypa::AstCallPtr& call_ast) {
  OperatorContext op_context({}, "");
  return ProcessArgs(call_ast, op_context);
}
StatusOr<ArgMap> ASTVisitorImpl::ProcessArgs(const pypa::AstCallPtr& call_ast,
                                             const OperatorContext& op_context) {
  auto arg_ast = call_ast->arglist;
  ArgMap arg_map;

  for (const auto arg : arg_ast.arguments) {
    PL_ASSIGN_OR_RETURN(IRNode * value, ProcessData(arg, op_context));
    arg_map.args.push_back(value);
  }

  // Iterate through the keywords
  for (auto& k : arg_ast.keywords) {
    pypa::AstKeywordPtr kw_ptr = PYPA_PTR_CAST(Keyword, k);
    std::string key = GetNameAsString(kw_ptr->name);
    PL_ASSIGN_OR_RETURN(IRNode * value, ProcessData(kw_ptr->value, op_context));
    arg_map.kwargs[key] = value;
  }

  return arg_map;
}  // namespace compiler

StatusOr<QLObjectPtr> ASTVisitorImpl::LookupVariable(const pypa::AstPtr& ast,
                                                     const std::string& name) {
  auto find_name = var_table_.find(name);
  if (find_name == var_table_.end()) {
    return CreateAstError(ast, "Can't find variable '$0'.", name);
  }
  return find_name->second;
}

StatusOr<OperatorIR*> ASTVisitorImpl::LookupName(const pypa::AstNamePtr& name_node) {
  PL_ASSIGN_OR_RETURN(QLObjectPtr pyobject, LookupVariable(name_node));
  if (!pyobject->HasNode()) {
    return CreateAstError(name_node, "'$0' not accessible", name_node->id);
  }
  IRNode* node = pyobject->node();
  if (!node->IsOperator()) {
    return node->CreateIRNodeError("Only dataframes may be assigned variables, $0 not allowed",
                                   node->type_string());
  }
  return static_cast<OperatorIR*>(node);
}

StatusOr<QLObjectPtr> ASTVisitorImpl::ProcessAttributeValue(const pypa::AstAttributePtr& node) {
  switch (node->value->type) {
    case AstType::Call: {
      return ProcessOpCallNode(PYPA_PTR_CAST(Call, node->value));
    }
    case AstType::Name: {
      return LookupVariable(PYPA_PTR_CAST(Name, node->value));
    }
    default: {
      return CreateAstError(node->value, "Can't handle the attribute of type $0",
                            GetAstTypeName(node->type));
    }
  }
}

StatusOr<std::string> ASTVisitorImpl::GetAttribute(const pypa::AstAttributePtr& attr) {
  if (attr->attribute->type != AstType::Name) {
    return CreateAstError(attr, "$0 not a valid attribute", GetAstTypeName(attr->attribute->type));
  }
  return GetNameAsString(attr->attribute);
}

IRNode* GetArgument(const ArgMap& args, const std::string& arg_name) {
  auto iter = args.kwargs.find(arg_name);
  if (iter == args.kwargs.end()) {
    return nullptr;
  }
  return iter->second;
}

StatusOr<QLObjectPtr> ASTVisitorImpl::ProcessOpCallNode(const pypa::AstCallPtr& node) {
  std::shared_ptr<FuncObject> func_object;
  // pyobject declared up here because we need this object to be allocated when
  // func_object->Call() is made.
  QLObjectPtr pyobject;
  switch (node->function->type) {
    case AstType::Attribute: {
      const pypa::AstAttributePtr& attr = PYPA_PTR_CAST(Attribute, node->function);
      PL_ASSIGN_OR_RETURN(pyobject, ProcessAttributeValue(attr));
      PL_ASSIGN_OR_RETURN(std::string attr_name, GetAttribute(attr));
      PL_ASSIGN_OR_RETURN(func_object, pyobject->GetMethod(attr_name));
      break;
    }
    case AstType::Name: {
      PL_ASSIGN_OR_RETURN(pyobject, LookupVariable(PYPA_PTR_CAST(Name, node->function)));
      if (pyobject->type_descriptor().type() != QLObjectType::kFunction) {
        PL_ASSIGN_OR_RETURN(func_object, pyobject->GetCallMethod());
        break;
      }
      func_object = std::static_pointer_cast<FuncObject>(pyobject);
      break;
    }
    default: {
      return CreateAstError(node, "'$0' object is not callable.",
                            GetAstTypeName(node->function->type));
    }
  }
  PL_ASSIGN_OR_RETURN(ArgMap args, ProcessArgs(node));
  return func_object->Call(args, node, this);
}

StatusOr<ExpressionIR*> ASTVisitorImpl::ProcessStr(const pypa::AstStrPtr& ast) {
  PL_ASSIGN_OR_RETURN(StringIR * ir_node, ir_graph_->MakeNode<StringIR>());
  PL_ASSIGN_OR_RETURN(auto str_value, GetStrAstValue(ast));
  PL_RETURN_IF_ERROR(ir_node->Init(str_value, ast));
  return ir_node;
}

Status ASTVisitorImpl::InitCollectionData(CollectionIR* collection, const pypa::AstPtr& ast,
                                          const pypa::AstExprList& elements,
                                          const OperatorContext& op_context) {
  std::vector<ExpressionIR*> children;
  for (auto& child : elements) {
    PL_ASSIGN_OR_RETURN(IRNode * child_node, ProcessData(child, op_context));
    if (!child_node->IsExpression()) {
      return CreateAstError(ast, "Can't support '$0' as a Collection member.",
                            child_node->type_string());
    }
    children.push_back(static_cast<ExpressionIR*>(child_node));
  }
  PL_RETURN_IF_ERROR(collection->Init(ast, children));
  return Status::OK();
}

StatusOr<ListIR*> ASTVisitorImpl::ProcessList(const pypa::AstListPtr& ast,
                                              const OperatorContext& op_context) {
  ListIR* ir_node = ir_graph_->MakeNode<ListIR>().ValueOrDie();
  PL_RETURN_IF_ERROR(InitCollectionData(ir_node, ast, ast->elements, op_context));
  return ir_node;
}

StatusOr<TupleIR*> ASTVisitorImpl::ProcessTuple(const pypa::AstTuplePtr& ast,
                                                const OperatorContext& op_context) {
  TupleIR* ir_node = ir_graph_->MakeNode<TupleIR>().ValueOrDie();
  PL_RETURN_IF_ERROR(InitCollectionData(ir_node, ast, ast->elements, op_context));
  return ir_node;
}

StatusOr<LambdaExprReturn> ASTVisitorImpl::ProcessLambdaNestedAttribute(
    const LambdaOperatorMap& arg_op_map, const std::string& attribute_value,
    const pypa::AstAttributePtr& parent_attr) {
  // The nested attribute only works with the format:
  // "<parent_attr.value>.<parent_attr.attribute>.<attribute_value>"

  // The following 3 checks make sure that parent_attr is "<value>.<attribute>".
  auto value = parent_attr->value;
  auto attribute = parent_attr->attribute;
  if (value->type != AstType::Name) {
    return CreateAstError(value, "Expected a Name here, got a $0", GetAstTypeName(value->type));
  }

  if (attribute->type != AstType::Name) {
    return CreateAstError(attribute, "Expected a Name here, got a $0",
                          GetAstTypeName(attribute->type));
  }
  std::string value_name = GetNameAsString(value);
  if (value_name != kCompileTimeFuncPrefix && value_name != kRunTimeFuncPrefix &&
      arg_op_map.find(value_name) == arg_op_map.end()) {
    return CreateAstError(value, "Name '$0' not defined.", value_name);
  }

  std::vector<std::string> expected_attrs = {kMDKeyword};
  // Find handled attribute keywords.
  if (GetNameAsString(attribute) == kMDKeyword) {
    // If attribute is the metadata keyword, then we process the metadata attribute.
    return ProcessMetadataAttribute(arg_op_map, attribute_value, parent_attr);
  }

  return CreateAstError(attribute, "Nested attribute can only be one of [$0]. '$1' not supported",
                        absl::StrJoin(expected_attrs, ","), GetNameAsString(attribute));
}

StatusOr<LambdaExprReturn> ASTVisitorImpl::ProcessMetadataAttribute(
    const LambdaOperatorMap& arg_op_map, const std::string& attribute_value,
    const pypa::AstAttributePtr& val_attr) {
  std::string value = GetNameAsString(val_attr->value);

  auto arg_op_iter = arg_op_map.find(value);
  if (arg_op_iter == arg_op_map.end()) {
    return CreateAstError(val_attr,
                          "Metadata call not allowed on '$0'. Must use a lambda argument "
                          "to access Metadata.",
                          value);
  }
  PL_ASSIGN_OR_RETURN(MetadataIR * md_node, ir_graph_->MakeNode<MetadataIR>());
  int64_t parent_op_idx = arg_op_iter->second;
  PL_RETURN_IF_ERROR(md_node->Init(attribute_value, parent_op_idx, val_attr->attribute));
  return LambdaExprReturn(md_node, {attribute_value});
}

StatusOr<LambdaExprReturn> ASTVisitorImpl::ProcessLambdaAttribute(
    const LambdaOperatorMap& arg_op_map, const pypa::AstAttributePtr& node) {
  // Attributes in libpypa follow the format: "<parent_value>.<attribute>".
  // Attribute should always be a name, but just in case it's not, we error out.
  if (node->attribute->type != AstType::Name) {
    return CreateAstError(node->attribute, "Attribute must be a Name not a $0.",
                          GetAstTypeName(node->attribute->type));
  }

  // Values of attributes (the parent of the attribute) can also be attributes themselves.
  // Nested attributes are handled here.
  if (node->value->type == AstType::Attribute) {
    return ProcessLambdaNestedAttribute(arg_op_map, GetNameAsString(node->attribute),
                                        PYPA_PTR_CAST(Attribute, node->value));
  }

  // If value is not an attribute, then it must be a name.
  if (node->value->type != AstType::Name) {
    return CreateAstError(node->value, "Attribute parent must be a name or an attribute, not a $0.",
                          GetAstTypeName(node->value->type));
  }

  std::string parent = GetNameAsString(node->value);
  if (parent == kRunTimeFuncPrefix) {
    // If the value of the attribute is kRunTimeFuncPrefix, then we have a function without
    // arguments.
    return ProcessArglessFunction(GetNameAsString(node->attribute));
  } else if (parent == kCompileTimeFuncPrefix) {
    // TODO(philkuz) should spend time figuring out how to make this work, logically it should.
    return CreateAstError(node, "'$0' not supported in lambda functions.", kCompileTimeFuncPrefix);
  } else if (arg_op_map.find(parent) != arg_op_map.end()) {
    // If the value is equal to the arg_op_map, then the attribute is a column.
    int64_t parent_op_idx = arg_op_map.find(parent)->second;
    return ProcessRecordColumn(GetNameAsString(node->attribute), node, parent_op_idx);
  }
  return CreateAstError(node, "Name '$0' is not defined.", parent);
}

StatusOr<LambdaExprReturn> ASTVisitorImpl::ProcessRecordColumn(const std::string& column_name,
                                                               const pypa::AstPtr& column_ast_node,
                                                               int64_t parent_op_idx) {
  std::unordered_set<std::string> column_names;
  column_names.insert(column_name);
  PL_ASSIGN_OR_RETURN(ColumnIR * column, ir_graph_->MakeNode<ColumnIR>());
  PL_RETURN_IF_ERROR(column->Init(column_name, parent_op_idx, column_ast_node));
  return LambdaExprReturn(column, column_names);
}

StatusOr<LambdaExprReturn> ASTVisitorImpl::ProcessArglessFunction(
    const std::string& function_name) {
  // TODO(philkuz) (PL-733) use this to remove the is_pixie_attr from LambdaExprReturn.
  return LambdaExprReturn(function_name, true /*is_pixie_attr */);
}

StatusOr<ExpressionIR*> ASTVisitorImpl::ProcessNumber(const pypa::AstNumberPtr& node) {
  switch (node->num_type) {
    case pypa::AstNumber::Type::Float: {
      PL_ASSIGN_OR_RETURN(FloatIR * ir_node, ir_graph_->MakeNode<FloatIR>());
      PL_RETURN_IF_ERROR(ir_node->Init(node->floating, node));
      return ir_node;
    }
    case pypa::AstNumber::Type::Integer:
    case pypa::AstNumber::Type::Long: {
      PL_ASSIGN_OR_RETURN(IntIR * ir_node, ir_graph_->MakeNode<IntIR>());
      PL_RETURN_IF_ERROR(ir_node->Init(node->integer, node));
      return ir_node;
    }
    default:
      return CreateAstError(node, "Couldn't find number type $0", node->num_type);
  }
}

StatusOr<LambdaExprReturn> ASTVisitorImpl::BuildLambdaFunc(
    const FuncIR::Op& op, const std::vector<LambdaExprReturn>& children_ret_expr,
    const pypa::AstPtr& parent_node) {
  PL_ASSIGN_OR_RETURN(FuncIR * ir_node, ir_graph_->MakeNode<FuncIR>());
  std::vector<ExpressionIR*> expressions;
  auto ret = LambdaExprReturn(ir_node);
  for (auto expr_ret : children_ret_expr) {
    if (expr_ret.StringOnly()) {
      return CreateAstError(parent_node, "Attribute call with '$0' prefix not allowed in lambda.",
                            kCompileTimeFuncPrefix);
    } else {
      expressions.push_back(expr_ret.expr_);
      ret.MergeColumns(expr_ret);
    }
  }
  PL_RETURN_IF_ERROR(ir_node->Init(op, expressions, parent_node));
  return ret;
}

StatusOr<LambdaExprReturn> ASTVisitorImpl::ProcessLambdaBinOp(const LambdaOperatorMap& arg_op_map,
                                                              const pypa::AstBinOpPtr& node) {
  std::string op_str = pypa::to_string(node->op);
  PL_ASSIGN_OR_RETURN(FuncIR::Op op, GetOp(op_str, node));
  std::vector<LambdaExprReturn> children_ret_expr;
  PL_ASSIGN_OR_RETURN(auto left_expr_ret, ProcessLambdaExpr(arg_op_map, node->left));
  PL_ASSIGN_OR_RETURN(auto right_expr_ret, ProcessLambdaExpr(arg_op_map, node->right));
  children_ret_expr.push_back(left_expr_ret);
  children_ret_expr.push_back(right_expr_ret);
  return BuildLambdaFunc(op, children_ret_expr, node);
}

StatusOr<LambdaExprReturn> ASTVisitorImpl::ProcessLambdaBoolOp(const LambdaOperatorMap& arg_op_map,
                                                               const pypa::AstBoolOpPtr& node) {
  std::string op_str = pypa::to_string(node->op);
  PL_ASSIGN_OR_RETURN(FuncIR::Op op, GetOp(op_str, node));
  std::vector<LambdaExprReturn> children_ret_expr;
  for (auto comp : node->values) {
    PL_ASSIGN_OR_RETURN(auto rt, ProcessLambdaExpr(arg_op_map, comp));
    children_ret_expr.push_back(rt);
  }
  return BuildLambdaFunc(op, children_ret_expr, node);
}

StatusOr<LambdaExprReturn> ASTVisitorImpl::ProcessLambdaCompare(const LambdaOperatorMap& arg_op_map,
                                                                const pypa::AstComparePtr& node) {
  DCHECK_EQ(node->operators.size(), 1ULL);
  std::string op_str = pypa::to_string(node->operators[0]);
  PL_ASSIGN_OR_RETURN(FuncIR::Op op, GetOp(op_str, node));
  std::vector<LambdaExprReturn> children_ret_expr;
  if (node->comparators.size() != 1) {
    return CreateAstError(node, "Only expected one argument to the right of '$0'.", op_str);
  }
  PL_ASSIGN_OR_RETURN(auto rt, ProcessLambdaExpr(arg_op_map, node->left));
  children_ret_expr.push_back(rt);
  for (auto comp : node->comparators) {
    PL_ASSIGN_OR_RETURN(auto rt, ProcessLambdaExpr(arg_op_map, comp));
    children_ret_expr.push_back(rt);
  }
  return BuildLambdaFunc(op, children_ret_expr, node);
}

StatusOr<LambdaExprReturn> ASTVisitorImpl::ProcessLambdaCall(const LambdaOperatorMap& arg_op_map,
                                                             const pypa::AstCallPtr& node) {
  PL_ASSIGN_OR_RETURN(auto attr_result, ProcessLambdaExpr(arg_op_map, node->function));
  if (!attr_result.StringOnly()) {
    return CreateAstError(node, "Expected a string for the return");
  }
  auto arglist = node->arglist;
  if (!arglist.defaults.empty() || !arglist.keywords.empty()) {
    return CreateAstError(node, "Only non-default and non-keyword args allowed.");
  }

  std::string fn_name = attr_result.str_;
  std::vector<LambdaExprReturn> children_ret_expr;
  for (auto arg_ast : arglist.arguments) {
    PL_ASSIGN_OR_RETURN(auto rt, ProcessLambdaExpr(arg_op_map, arg_ast));
    children_ret_expr.push_back(rt);
  }
  FuncIR::Op op{FuncIR::Opcode::non_op, "", fn_name};
  return BuildLambdaFunc(op, children_ret_expr, node);
}

/**
 * @brief Wraps an IrNode StatusOr return with the LambdaExprReturn StatusOr
 *
 * @param node
 * @return StatusOr<LambdaExprReturn>
 */
StatusOr<LambdaExprReturn> WrapLambdaExprReturn(StatusOr<ExpressionIR*> node) {
  PL_RETURN_IF_ERROR(node);
  return LambdaExprReturn(node.ValueOrDie());
}

StatusOr<LambdaExprReturn> ASTVisitorImpl::ProcessLambdaList(const LambdaOperatorMap& arg_op_map,
                                                             const pypa::AstListPtr& node) {
  ListIR* ir_node = ir_graph_->MakeNode<ListIR>().ValueOrDie();
  LambdaExprReturn expr_return(ir_node);
  std::vector<ExpressionIR*> children;
  for (auto& child : node->elements) {
    PL_ASSIGN_OR_RETURN(auto child_attr, ProcessLambdaExpr(arg_op_map, child));
    if (child_attr.StringOnly() || (!child_attr.expr_->IsColumn())) {
      return CreateAstError(
          node, "Lambda list can only handle Metadata and Column types. Can't handle $0.",
          child_attr.expr_->type_string());
    }
    expr_return.MergeColumns(child_attr);
    children.push_back(child_attr.expr_);
  }
  PL_RETURN_IF_ERROR(ir_node->Init(node, children));
  return expr_return;
}

StatusOr<LambdaExprReturn> ASTVisitorImpl::ProcessLambdaExpr(const LambdaOperatorMap& arg_op_map,
                                                             const pypa::AstPtr& node) {
  LambdaExprReturn expr_return;
  switch (node->type) {
    case AstType::BinOp: {
      PL_ASSIGN_OR_RETURN(expr_return, ProcessLambdaBinOp(arg_op_map, PYPA_PTR_CAST(BinOp, node)));
      break;
    }
    case AstType::Attribute: {
      PL_ASSIGN_OR_RETURN(expr_return,
                          ProcessLambdaAttribute(arg_op_map, PYPA_PTR_CAST(Attribute, node)));
      break;
    }
    case AstType::Number: {
      auto number_result = ProcessNumber(PYPA_PTR_CAST(Number, node));
      PL_ASSIGN_OR_RETURN(expr_return, WrapLambdaExprReturn(number_result));
      break;
    }
    case AstType::Str: {
      auto str_result = ProcessStr(PYPA_PTR_CAST(Str, node));
      PL_ASSIGN_OR_RETURN(expr_return, WrapLambdaExprReturn(str_result));
      break;
    }
    case AstType::Call: {
      PL_ASSIGN_OR_RETURN(expr_return, ProcessLambdaCall(arg_op_map, PYPA_PTR_CAST(Call, node)));
      break;
    }
    case AstType::List: {
      PL_ASSIGN_OR_RETURN(expr_return, ProcessLambdaList(arg_op_map, PYPA_PTR_CAST(List, node)));
      break;
    }
    case AstType::Compare: {
      PL_ASSIGN_OR_RETURN(expr_return,
                          ProcessLambdaCompare(arg_op_map, PYPA_PTR_CAST(Compare, node)));
      break;
    }
    case AstType::BoolOp: {
      PL_ASSIGN_OR_RETURN(expr_return,
                          ProcessLambdaBoolOp(arg_op_map, PYPA_PTR_CAST(BoolOp, node)));
      break;
    }
    default: {
      return CreateAstError(node, "Node of type $0 not allowed for expression in Lambda function.",
                            GetAstTypeName(node->type));
    }
  }
  return expr_return;
}

StatusOr<LambdaOperatorMap> ASTVisitorImpl::ProcessLambdaArgs(const pypa::AstLambdaPtr& node) {
  auto arg_ast = node->arguments;
  if (!arg_ast.defaults.empty() && arg_ast.defaults[0]) {
    return CreateAstError(node, "No default arguments allowed for lambdas. Found $0 default args.",
                          arg_ast.defaults.size());
  }
  if (!arg_ast.keywords.empty()) {
    return CreateAstError(node, "No keyword arguments allowed for lambdas.");
  }

  LambdaOperatorMap out_map;
  for (const auto& [idx, arg_node] : Enumerate(arg_ast.arguments)) {
    if (arg_node->type != AstType::Name) {
      return CreateAstError(node, "Argument must be a Name.");
    }
    std::string arg_str = GetNameAsString(arg_node);
    if (out_map.find(arg_str) != out_map.end()) {
      return CreateAstError(node, "Duplicate argument '$0' in lambda definition.", arg_str);
    }

    out_map.emplace(arg_str, /* parent_op_idx */ idx);
  }
  return out_map;
}

StatusOr<LambdaBodyReturn> ASTVisitorImpl::ProcessLambdaDict(const LambdaOperatorMap& arg_op_map,
                                                             const pypa::AstDictPtr& body_dict) {
  auto return_val = LambdaBodyReturn();
  for (size_t i = 0; i < body_dict->keys.size(); i++) {
    auto key_str_ast = body_dict->keys[i];
    auto value_ast = body_dict->values[i];
    PL_ASSIGN_OR_RETURN(auto key_string, GetStrAstValue(key_str_ast));
    PL_ASSIGN_OR_RETURN(auto expr_ret, ProcessLambdaExpr(arg_op_map, value_ast));
    if (expr_ret.is_pixie_attr_) {
      PL_ASSIGN_OR_RETURN(
          expr_ret, BuildLambdaFunc({FuncIR::Opcode::non_op, "", expr_ret.str_}, {}, body_dict));
    }
    PL_RETURN_IF_ERROR(return_val.AddExprResult(key_string, expr_ret));
  }
  return return_val;
}

StatusOr<LambdaIR*> ASTVisitorImpl::ProcessLambda(const pypa::AstLambdaPtr& ast,
                                                  const OperatorContext&) {
  LambdaIR* lambda_node = ir_graph_->MakeNode<LambdaIR>().ValueOrDie();
  PL_ASSIGN_OR_RETURN(LambdaOperatorMap arg_op_map, ProcessLambdaArgs(ast));
  LambdaBodyReturn return_struct;
  switch (ast->body->type) {
    case AstType::Dict: {
      PL_ASSIGN_OR_RETURN(return_struct,
                          ProcessLambdaDict(arg_op_map, PYPA_PTR_CAST(Dict, ast->body)));
      PL_RETURN_IF_ERROR(lambda_node->Init(return_struct.input_relation_columns_,
                                           return_struct.col_exprs_, ast->body));
      return lambda_node;
    }

    default: {
      PL_ASSIGN_OR_RETURN(LambdaExprReturn return_val, ProcessLambdaExpr(arg_op_map, ast->body));
      PL_RETURN_IF_ERROR(
          lambda_node->Init(return_val.input_relation_columns_, return_val.expr_, ast->body));
      return lambda_node;
    }
  }
}

StatusOr<ExpressionIR*> ASTVisitorImpl::ProcessDataBinOp(const pypa::AstBinOpPtr& node,
                                                         const OperatorContext& op_context) {
  std::string op_str = pypa::to_string(node->op);
  PL_ASSIGN_OR_RETURN(FuncIR::Op op, GetOp(op_str, node));

  PL_ASSIGN_OR_RETURN(IRNode * left, ProcessData(node->left, op_context));
  PL_ASSIGN_OR_RETURN(IRNode * right, ProcessData(node->right, op_context));
  if (!left->IsExpression()) {
    return CreateAstError(
        node,
        "Expected left side of operation to be an expression, but got $0, which is not an "
        "expression..",
        left->type_string());
  }
  if (!right->IsExpression()) {
    return CreateAstError(
        node,
        "Expected right side of operation to be an expression, but got $0, which is not an "
        "expression.",
        right->type_string());
  }
  PL_ASSIGN_OR_RETURN(FuncIR * ir_node, ir_graph_->MakeNode<FuncIR>());
  std::vector<ExpressionIR*> expressions = {static_cast<ExpressionIR*>(left),
                                            static_cast<ExpressionIR*>(right)};
  PL_RETURN_IF_ERROR(ir_node->Init(op, expressions, node));

  return ir_node;
}

StatusOr<ColumnIR*> ASTVisitorImpl::ProcessSubscriptColumn(const pypa::AstSubscriptPtr& subscript,
                                                           const OperatorContext& op_context) {
  auto value_ir = ProcessData(subscript->value, op_context);

  // TODO(nserrino) support indexing into lists and other things like that.
  if (subscript->value->type != AstType::Name) {
    return CreateAstError(subscript->value,
                          "Subscript is only currently supported on dataframes, received $0.",
                          GetAstTypeName(subscript->value->type));
  }

  auto name = PYPA_PTR_CAST(Name, subscript->value);
  if (std::find(op_context.referenceable_dataframes.begin(),
                op_context.referenceable_dataframes.end(),
                name->id) == op_context.referenceable_dataframes.end()) {
    return CreateAstError(name, "Name $0 is not available in this context", name->id);
  }

  if (subscript->slice->type != AstType::Index) {
    return CreateAstError(subscript->slice,
                          "Expected to receive index as subscript slice, received $0.",
                          GetAstTypeName(subscript->slice->type));
  }
  auto index = PYPA_PTR_CAST(Index, subscript->slice);
  if (index->value->type != AstType::Str) {
    return CreateAstError(index->value,
                          "Expected to receive string as subscript index value, received $0.",
                          GetAstTypeName(index->value->type));
  }
  auto col_name = PYPA_PTR_CAST(Str, index->value)->value;

  PL_ASSIGN_OR_RETURN(ColumnIR * subscript_col, ir_graph_->MakeNode<ColumnIR>());
  PL_RETURN_IF_ERROR(subscript_col->Init(col_name, /* parent_op_idx */ 0, subscript));
  return subscript_col;
}

StatusOr<ExpressionIR*> ASTVisitorImpl::ProcessDataCall(const pypa::AstCallPtr& node,
                                                        const OperatorContext& op_context) {
  auto fn = node->function;
  if (fn->type != AstType::Attribute) {
    return CreateAstError(fn, "Expected any function calls to be made an attribute, not as a $0",
                          GetAstTypeName(fn->type));
  }
  auto fn_attr = PYPA_PTR_CAST(Attribute, fn);
  auto attr_parent = fn_attr->value;
  auto attr_fn_name = fn_attr->attribute;
  if (attr_parent->type != AstType::Name) {
    return CreateAstError(attr_parent, "Nested calls not allowed. Expected Name attr not $0",
                          GetAstTypeName(fn->type));
  }
  if (attr_fn_name->type != AstType::Name) {
    return CreateAstError(attr_fn_name, "Expected Name attr not $0", GetAstTypeName(fn->type));
  }
  // attr parent must be plc.
  auto attr_parent_str = GetNameAsString(attr_parent);
  if (attr_parent_str != kCompileTimeFuncPrefix && attr_parent_str != kRunTimeFuncPrefix) {
    return CreateAstError(attr_parent, "Namespace '$0' not found.", attr_parent_str);
  }
  auto attr_fn_str = GetNameAsString(attr_fn_name);

  std::vector<ExpressionIR*> func_args;
  // TODO(nserrino,philkuz) support keyword args in data calls once PyObjects land.
  for (const auto& pypa_arg : node->arglist.arguments) {
    PL_ASSIGN_OR_RETURN(auto result, ProcessData(pypa_arg, op_context));
    if (!result->IsExpression()) {
      return CreateAstError(pypa_arg, "Expected argument to function '$0' to be an expression",
                            attr_fn_str);
    }
    func_args.push_back(static_cast<ExpressionIR*>(result));
  }

  PL_ASSIGN_OR_RETURN(FuncIR * func_ir, ir_graph_->MakeNode<FuncIR>());
  FuncIR::Op op{FuncIR::Opcode::non_op, "", attr_fn_str};
  PL_RETURN_IF_ERROR(func_ir->Init(op, func_args, node));

  return func_ir;
}

StatusOr<IRNode*> ASTVisitorImpl::ProcessData(const pypa::AstPtr& ast,
                                              const OperatorContext& op_context) {
  switch (ast->type) {
    case AstType::Str: {
      return ProcessStr(PYPA_PTR_CAST(Str, ast));
    }
    case AstType::Number: {
      return ProcessNumber(PYPA_PTR_CAST(Number, ast));
    }
    case AstType::List: {
      return ProcessList(PYPA_PTR_CAST(List, ast), op_context);
    }
    case AstType::Tuple: {
      return ProcessTuple(PYPA_PTR_CAST(Tuple, ast), op_context);
    }
    case AstType::Lambda: {
      return ProcessLambda(PYPA_PTR_CAST(Lambda, ast), op_context);
    }
    case AstType::Call: {
      return ProcessDataCall(PYPA_PTR_CAST(Call, ast), op_context);
    }
    case AstType::BinOp: {
      return ProcessDataBinOp(PYPA_PTR_CAST(BinOp, ast), op_context);
    }
    case AstType::Name: {
      return LookupName(PYPA_PTR_CAST(Name, ast));
    }
    case AstType::Subscript: {
      return ProcessSubscriptColumn(PYPA_PTR_CAST(Subscript, ast), op_context);
    }
    default: {
      return CreateAstError(ast, "Couldn't find $0 in ProcessData", GetAstTypeName(ast->type));
    }
  }
}
}  // namespace compiler
}  // namespace carnot
}  // namespace pl
