#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t offset = 0;

  // Serialize column name length and name
  uint32_t name_len = name_.size();
  memcpy(buf + offset, &name_len, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  memcpy(buf + offset, name_.c_str(), name_len);
  offset += name_len;

  // Serialize type
  memcpy(buf + offset, &type_, sizeof(TypeId));
  offset += sizeof(TypeId);

  // Serialize length
  memcpy(buf + offset, &len_, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  // Serialize table index
  memcpy(buf + offset, &table_ind_, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  // Serialize nullable flag
  memcpy(buf + offset, &nullable_, sizeof(bool));
  offset += sizeof(bool);

  // Serialize unique flag
  memcpy(buf + offset, &unique_, sizeof(bool));
  offset += sizeof(bool);

  return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  uint32_t size = 0;

  // Column name length
  size += sizeof(uint32_t);
  size += name_.size();

  // Type
  size += Type::GetTypeSize(type_);

  // Length
  size += sizeof(uint32_t);

  // Table index
  size += sizeof(uint32_t);

  // Nullable flag
  size += sizeof(bool);

  // Unique flag
  size += sizeof(bool);

  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t offset = 0;

  // Deserialize column name length and name
  uint32_t name_len;
  memcpy(&name_len, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  std::string column_name(buf + offset, name_len);
  offset += name_len;

  // Deserialize type
  TypeId type;
  memcpy(&type, buf + offset, sizeof(TypeId));
  offset += sizeof(TypeId);

  // Deserialize length
  uint32_t length;
  memcpy(&length, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  // Deserialize table index
  uint32_t table_ind;
  memcpy(&table_ind, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  // Deserialize nullable flag
  bool nullable;
  memcpy(&nullable, buf + offset, sizeof(bool));
  offset += sizeof(bool);

  // Deserialize unique flag
  bool unique;
  memcpy(&unique, buf + offset, sizeof(bool));
  offset += sizeof(bool);

  // Create a new Column object
  if (type == TypeId::kTypeChar) {
    column = new Column(column_name, type, length, table_ind, nullable, unique);
  } else {
    column = new Column(column_name, type, table_ind, nullable, unique);
  }

  return offset;
}
