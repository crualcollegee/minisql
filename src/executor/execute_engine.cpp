#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

#include <fstream>
#include "parser/parser.h"

extern "C" {
  typedef struct yy_buffer_state* YY_BUFFER_STATE;
  YY_BUFFER_STATE yy_scan_string(const char *); 
  void yy_delete_buffer(YY_BUFFER_STATE);
  int yyparse();
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  // todo:: use shared_ptr for schema
  if (ast->type_ == kNodeSelect)
      delete planner.plan_->OutputSchema();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 * 先找到ast的最后，将uniquekey记录下来
 * 然后读取每一个column,组成columns构建schema
 * 根据 uniquekey和primarykey 分别构建索引树
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_.empty()){
    return DB_FAILED;
  }
  auto ast_1 = ast->child_; 
  string tableName;
  if (ast_1 == nullptr||ast_1->type_!=kNodeIdentifier){
    return DB_FAILED;
  }
  else{
    tableName = string(ast_1->val_);
  }
  auto ast_2 = ast_1->next_;
  if (ast_2 == nullptr||ast_2->type_!=kNodeColumnDefinitionList){
    return DB_FAILED;
  }
  
  auto iter_ast = ast_2->child_;
  while (iter_ast!=nullptr && iter_ast->type_!=kNodeColumnList){
    iter_ast = iter_ast->next_;
  }
  vector<string> primaryList;
  vector<string> uniqueList;
  vector<Column*> ColumnList;
  if (iter_ast == nullptr || iter_ast->type_!=kNodeColumnList || 
    iter_ast->child_==nullptr || iter_ast->child_->type_!=kNodeIdentifier){
    return DB_FAILED;
  }
  else{
    auto primary_iter_ast = iter_ast->child_;
    while(primary_iter_ast!=nullptr){
      primaryList.push_back(primary_iter_ast->val_);
      primary_iter_ast = primary_iter_ast->next_;
    }
  }
  
  int index_l = 0;
  auto ast_l = ast_2;
  while (ast_l->type_!=kNodeColumnList){
    string constraint = string(ast_l->val_);
    bool nullable = !(constraint == "not null" || constraint == "NOT NULL");
    bool isunique = (constraint == "unique" || constraint == "UNIQUE");
    

    auto ast_child_l = ast_l->child_;
    string name_l = string(ast_child_l->val_);

    ast_child_l = ast_child_l->next_;
    string column_type = string(ast_child_l->val_);
    Column* column_l;
    if (column_type == "int"){
      column_l = new Column(name_l,kTypeInt,index_l++,nullable,isunique);
    }
    else if (column_type == "float"){
      column_l = new Column(name_l,kTypeFloat,index_l++,nullable,isunique);
    }
    else if(column_type == "char"){
      ast_child_l = ast_child_l->next_;
      int length = stoi(string(ast_child_l->val_));
      column_l = new Column(name_l,kTypeChar,length,index_l++,nullable,isunique);
    }
    ColumnList.push_back(column_l);
  }

  Schema* schema_l = new Schema(ColumnList);
  auto catalogManager = context->GetCatalog();
  auto Txn = context->GetTransaction();

  TableInfo* tableinfo_l;
  auto result = catalogManager->CreateTable(tableName,schema_l,Txn,tableinfo_l);

  if (!uniqueList.empty()){
    for (int i=0;i<uniqueList.size();i++){
      string indexID = uniqueList[i];
      string indexName = "index_" + indexID + "_on_" + tableName;
      vector<string> uniqueIndex;
      uniqueIndex.push_back(indexID);
      IndexInfo* indexInfo_l;
      catalogManager->CreateIndex(tableName,indexName,uniqueIndex,Txn,indexInfo_l,"btree");
    }
  }

  if (!primaryList.empty()){
    string indexName = "index_";
    for (auto &it : primaryList){
      indexName += it;
    }
    indexName += "_on_" + tableName;
    IndexInfo* indexInfo_l;
    catalogManager->CreateIndex(tableName,indexName,primaryList,Txn,indexInfo_l,"btree");
  }
  return result;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (current_db_.empty()){
    return DB_FAILED;
  }
  else{
    auto ast_child = ast->child_;
    if (ast_child==nullptr){
      return DB_FAILED;
    }
    string table_name = string(ast_child->val_);

    auto CatalogManager = context->GetCatalog();
    auto Txn = context->GetTransaction();

    vector<IndexInfo*> indexes;
    CatalogManager->GetTableIndexes(table_name,indexes);
    for (auto &it:indexes){
      CatalogManager->DropIndex(table_name,it->GetIndexName());
    }
    return CatalogManager->DropTable(table_name);
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()){
    return DB_FAILED;
  }
  auto CatalogManager = context->GetCatalog();
  vector<TableInfo*> TableInfo_l;
  vector<IndexInfo*> IndexInfo_l;
  CatalogManager->GetTables(TableInfo_l);
  uint32_t i=0;
  vector<uint32_t> TableRecorder;
  for (auto &a:TableInfo_l){
    if (i){
      TableRecorder.push_back(i);
    }
    CatalogManager->GetTableIndexes(a->GetTableName(),IndexInfo_l);
    i = IndexInfo_l.size();
  }

  stringstream ss;
  ResultWriter writer(ss);
  vector<int> width;
  width.push_back(5);

  for (auto &x:IndexInfo_l){
    width[0] = max(width[0],(int)(x->GetIndexName().length()));
  }

  writer.Divider(width);
  writer.BeginRow();
  writer.WriteHeaderCell("Index", width[0]);
  writer.EndRow();
  writer.Divider(width);
  
  int j=0;
  for (int i=0;i<IndexInfo_l.size();i++){
    if (j<TableRecorder.size() && i==TableRecorder[j]){
      writer.BeginRow();
      writer.EndRow();
      j++;
    }
    writer.BeginRow();
    writer.WriteCell(IndexInfo_l[i]->GetIndexName(), width[0]);
    writer.EndRow();
  }

  writer.Divider(width);
  cout << writer.stream_.rdbuf();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (current_db_.empty()){
    return DB_FAILED;
  }
  auto ast_index = ast->child_;
  if (ast_index==nullptr||ast_index->type_!=kNodeIdentifier){
    return DB_FAILED;
  }
  string IndexName = string(ast_index->val_);

  auto ast_table = ast_index->next_;
  if (ast_table==nullptr||ast_table->type_!=kNodeIdentifier){
    return DB_FAILED;
  }
  string TableName = string(ast_table->val_);
  
  auto ast_keyLists = ast_table->next_;
  if (ast_keyLists==nullptr||ast_keyLists->type_!=kNodeColumnList){
    return DB_FAILED;
  }
  auto ast_keyNode = ast_keyLists->child_;
  if (ast_keyNode==nullptr||ast_keyNode->type_!=kNodeIdentifier){
    return DB_FAILED;
  }

  vector<string> IndexLists;
  while(ast_keyNode!=nullptr){
    IndexLists.push_back(string(ast_keyNode->val_));
    ast_keyNode = ast_keyNode->next_;
  }
  auto CatalogManager = context->GetCatalog();
  auto Txn = context->GetTransaction();
  IndexInfo* IndexInfo_l;
  auto result = CatalogManager->CreateIndex(TableName,IndexName,IndexLists,Txn,IndexInfo_l,"btree");
  return result;
}

/*
  if (!primaryList.empty()){
    string indexName = "index_";
    for (auto &it : primaryList){
      indexName += it;
    }
    indexName += "_on_" + tableName;
    IndexInfo* indexInfo_l;
    catalogManager->CreateIndex(tableName,indexName,primaryList,Txn,indexInfo_l,"btree");
  }
  return result;
*/



/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (current_db_.empty()){
    return DB_FAILED;
  }
  auto ast_child = ast->child_;
  if (ast_child==nullptr||ast_child->type_!=kNodeIdentifier){
    return DB_FAILED;
  }
  string IndexName = string(ast_child->val_);

  auto CatalogManager = context->GetCatalog();
  vector<TableInfo*> TableInfoList;
  CatalogManager->GetTables(TableInfoList);
  dberr_t res = DB_FAILED;
  for (auto &x:TableInfoList){
    bool flag = CatalogManager->DropIndex(x->GetTableName(),IndexName);
    if (flag)res = DB_SUCCESS;
  }
  return res;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
  // 必须先选中数据库
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  // 从 AST 获取文件名
  if (ast->child_ == nullptr || ast->child_->type_ != kNodeString) {
    cout << "Invalid EXECFILE syntax" << endl;
    return DB_FAILED;
  }
  string filename = ast->child_->val_;
  // 构造脚本文件的完整路径：./databases/<db>/<filename>
  string fullpath = string("./databases/") + current_db_ + "/" + filename;
  ifstream file(fullpath);
  if (!file.is_open()) {
    cout << "Failed to open file: " << fullpath << endl;
    return DB_FAILED;
  }
  string line;
  while (getline(file, line)) {
    // 跳过空行或纯注释
    if (line.empty()) continue;
    // 初始化解析器状态
    MinisqlParserInit();
    // 将这一行传给解析器
    yy_scan_string(line.c_str());
    yyparse();
    // 解析错误
    if (MinisqlParserGetError()) {
      cout << MinisqlParserGetErrorMessage() << endl;
      MinisqlParserFinish();
      return DB_FAILED;
    }
    // 获取 AST 并判断是否是 QUIT
    pSyntaxNode root = MinisqlGetParserRootNode();
    if (root->type_ == kNodeQuit) {
      MinisqlParserFinish();
      return DB_QUIT;
    }
    // 执行语句
    dberr_t rc = Execute(root);
    ExecuteInformation(rc);
    MinisqlParserFinish();
    if (rc == DB_QUIT) {
      return DB_QUIT;
    }
  }
  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  current_db_ = "";
  return DB_SUCCESS;
}
