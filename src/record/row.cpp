#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

  uint32_t offset = 0;

  // 写入字段数量
  uint32_t field_count = fields_.size();
  memcpy(buf + offset, &field_count, sizeof(field_count));
  offset += sizeof(field_count);
  // 写入空值位图
  uint32_t null_bitmap_size = (field_count + 7) / 8; // 每8个字段占1字节
  std::vector<uint8_t> null_bitmap(null_bitmap_size, 0);
  for (uint32_t i = 0; i < field_count; i++) {
    if (fields_[i]->IsNull()) {// 如果字段是空值
      null_bitmap[i / 8] |= (1 << (i % 8));
    }
  }
  memcpy(buf + offset, null_bitmap.data(), null_bitmap_size);
  offset += null_bitmap_size;
  // 写入每个字段的数据
  for (uint32_t i = 0; i < field_count; i++) {
    if (!fields_[i]->IsNull()) {
      int tmp2 = fields_[i]->SerializeTo(buf + offset);
      offset += tmp2;
    }
  }
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before deserialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");

  uint32_t offset = 0;

  // 读取字段数量
  uint32_t field_count = 0;
  memcpy(&field_count, buf + offset, sizeof(field_count));
  offset += sizeof(field_count);
  // 读取空值位图
  uint32_t null_bitmap_size = (field_count + 7) / 8;
  std::vector<uint8_t> null_bitmap(null_bitmap_size, 0);
  memcpy(null_bitmap.data(), buf + offset, null_bitmap_size);
  offset += null_bitmap_size;
  // 读取每个字段的数据
  fields_.resize(field_count, nullptr);
  for (uint32_t i = 0; i < field_count; i++) {
    if (null_bitmap[i / 8] & (1 << (i % 8))) {
      // 如果字段是空值
      fields_[i] = new Field(schema->GetColumn(i)->GetType());
    } else {
      // 如果字段有值
      fields_[i] = new Field(schema->GetColumn(i)->GetType());
      int tmp = Field::DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &fields_[i], 0);
      offset += tmp;
    }
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before calculating serialized size.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

  uint32_t size = 0;

  // 字段数量大小
  size += sizeof(uint32_t);

  // 空值位图大小
  uint32_t field_count = fields_.size();
  size += (field_count + 7) / 8;

  // 每个字段的序列化大小
  for (const auto &field : fields_) {
    if (!field->IsNull()) {
      size += field->GetSerializedSize();
    }
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
