// src/wal/record.cpp

#include "../../include/wal/record.h"
#include <cstddef>
#include <cstring>

namespace toni_lsm {

Record Record::createRecord(uint64_t tranc_id) {
  // TODO: Lab 5.3 实现创建事务的Record
  return {};
}
Record Record::commitRecord(uint64_t tranc_id) {
  // TODO: Lab 5.3 实现提交事务的Record
  return {};
}
Record Record::rollbackRecord(uint64_t tranc_id) {
  // TODO: Lab 5.3 实现回滚事务的Record
  return {};
}
Record Record::putRecord(uint64_t tranc_id, const std::string &key,
                         const std::string &value) {
  // TODO: Lab 5.3 实现插入键值对的Record
  return {};
}
Record Record::deleteRecord(uint64_t tranc_id, const std::string &key) {
  // TODO: Lab 5.3 实现删除键值对的Record
  return {};
}

std::vector<uint8_t> Record::encode() const {
  // TODO: Lab 5.3 实现Record的编码函数
  return {};
}

std::vector<Record> Record::decode(const std::vector<uint8_t> &data) {
  // TODO: Lab 5.3 实现Record的解码函数
  return {};
}
void Record::print() const {
  std::cout << "Record: tranc_id=" << tranc_id_
            << ", operation_type=" << static_cast<int>(operation_type_)
            << ", key=" << key_ << ", value=" << value_ << std::endl;
}

bool Record::operator==(const Record &other) const {
  if (tranc_id_ != other.tranc_id_ ||
      operation_type_ != other.operation_type_) {
    return false;
  }

  // 不需要 key 和 value 比较的情况
  if (operation_type_ == OperationType::CREATE ||
      operation_type_ == OperationType::COMMIT ||
      operation_type_ == OperationType::ROLLBACK) {
    return true;
  }

  // 需要 key 比较的情况
  if (operation_type_ == OperationType::DELETE) {
    return key_ == other.key_;
  }

  // 需要 key 和 value 比较的情况
  return key_ == other.key_ && value_ == other.value_;
}

bool Record::operator!=(const Record &other) const { return !(*this == other); }
} // namespace toni_lsm