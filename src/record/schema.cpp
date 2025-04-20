#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  // 写入魔数
  uint32_t offset = 0;
  memcpy(buf + offset, &SCHEMA_MAGIC_NUM, sizeof(SCHEMA_MAGIC_NUM));
  offset += sizeof(SCHEMA_MAGIC_NUM);

  // 写入列的数量
  uint32_t column_count = GetColumnCount();
  memcpy(buf + offset, &column_count, sizeof(column_count));
  offset += sizeof(column_count);

  // 写入每个列的序列化数据
  for (const auto &column : columns_) {
    offset += column->SerializeTo(buf + offset);
  }

  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = sizeof(SCHEMA_MAGIC_NUM) + sizeof(uint32_t); // 魔数 + 列数量
  for (const auto &column : columns_) {
    size += column->GetSerializedSize(); // 每列的序列化大小
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t offset = 0;

  // 读取魔数并校验
  uint32_t magic_num = 0;
  memcpy(&magic_num, buf + offset, sizeof(SCHEMA_MAGIC_NUM));
  offset += sizeof(SCHEMA_MAGIC_NUM);
  if (magic_num != SCHEMA_MAGIC_NUM) {
    LOG(ERROR) << "Schema magic number mismatch!";
    return 0;
  }

  // 读取列的数量
  uint32_t column_count = 0;
  memcpy(&column_count, buf + offset, sizeof(column_count));
  offset += sizeof(column_count);

  // 读取每个列的数据
  std::vector<Column *> columns;
  for (uint32_t i = 0; i < column_count; ++i) {
    Column *column = nullptr;
    offset += Column::DeserializeFrom(buf + offset, column);
    columns.push_back(column);
  }

  // 创建Schema对象
  schema = new Schema(columns, true);
  return offset;
}