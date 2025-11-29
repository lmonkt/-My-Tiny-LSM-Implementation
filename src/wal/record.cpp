// src/wal/record.cpp

#include "../../include/wal/record.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace tiny_lsm {

Record Record::createRecord(uint64_t tranc_id) {
  // TODO: Lab 5.3 实现创建事务的Record
  auto rec = Record();
  rec.operation_type_ = OperationType::CREATE;
  rec.tranc_id_ = tranc_id;
  rec.record_len_ = sizeof(uint16_t) + sizeof(uint64_t) + sizeof(OperationType);
  return rec;
}
Record Record::commitRecord(uint64_t tranc_id) {
  // TODO: Lab 5.3 实现提交事务的Record
  auto rec = Record();
  rec.operation_type_ = OperationType::COMMIT;
  rec.tranc_id_ = tranc_id;
  rec.record_len_ = sizeof(uint16_t) + sizeof(uint64_t) + sizeof(OperationType);
  return rec;
}
Record Record::rollbackRecord(uint64_t tranc_id) {
  // TODO: Lab 5.3 实现回滚事务的Record
  auto rec = Record();
  rec.operation_type_ = OperationType::ROLLBACK;
  rec.tranc_id_ = tranc_id;
  rec.record_len_ = sizeof(uint16_t) + sizeof(uint64_t) + sizeof(OperationType);
  return rec;
}
Record Record::putRecord(uint64_t tranc_id, const std::string &key,
                         const std::string &value) {
  // TODO: Lab 5.3 实现插入键值对的Record
  auto rec = Record();
  rec.operation_type_ = OperationType::PUT;
  rec.tranc_id_ = tranc_id;
  rec.key_ = key;
  rec.value_ = value;
  rec.record_len_ = sizeof(uint16_t) +      // record_len 字段
                    sizeof(uint64_t) +      // tranc_id
                    sizeof(OperationType) + // operation_type
                    sizeof(uint16_t) +      // key_len 字段
                    rec.key_.size() +       // key 的实际数据长度
                    sizeof(uint16_t) + rec.value_.size();
  return rec;
}
Record Record::deleteRecord(uint64_t tranc_id, const std::string &key) {
  // TODO: Lab 5.3 实现删除键值对的Record
  auto rec = Record();
  rec.operation_type_ = OperationType::DELETE;
  rec.tranc_id_ = tranc_id;
  rec.key_ = key;
  rec.record_len_ = sizeof(uint16_t) +      // record_len 字段
                    sizeof(uint64_t) +      // tranc_id
                    sizeof(OperationType) + // operation_type
                    sizeof(uint16_t) +      // key_len 字段
                    rec.key_.size();        // key 的实际数据长度
  return rec;
}

std::vector<uint8_t> Record::encode() const {
  // TODO: Lab 5.3 实现Record的编码函数
  std::vector<uint8_t> data(this->record_len_, 0);
  uint8_t *pos = data.data();

  memcpy(pos, &record_len_, sizeof(uint16_t));
  pos += sizeof(uint16_t);

  memcpy(pos, &tranc_id_, sizeof(uint64_t));
  pos += sizeof(uint64_t);

  memcpy(pos, &operation_type_, sizeof(OperationType));
  pos += sizeof(OperationType);

  if (operation_type_ == OperationType::PUT ||
      operation_type_ == OperationType::DELETE) {
    uint16_t str_len = key_.size();
    memcpy(pos, &str_len, sizeof(uint16_t));
    pos += sizeof(uint16_t);
    memcpy(pos, key_.data(), str_len);
    pos += str_len;
  }
  if (operation_type_ == OperationType::PUT) {
    uint16_t str_len = value_.size();
    memcpy(pos, &str_len, sizeof(uint16_t));
    pos += sizeof(uint16_t);
    memcpy(pos, value_.data(), str_len);
    pos += str_len;
  }
  return data;
}

std::vector<Record> Record::decode(const std::vector<uint8_t> &data) {
  // TODO: Lab 5.3 实现Record的解码函数
  const uint8_t *pos = data.data();
  const uint8_t *end = data.data() + data.size();
  std::vector<Record> res;
  while (pos < end) {
    Record record;
    uint16_t rec_len;
    uint64_t trac_id;
    OperationType ot;
    memcpy(&record.record_len_, pos, sizeof(uint16_t));
    pos += sizeof(uint16_t);

    memcpy(&record.tranc_id_, pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    memcpy(&record.operation_type_, pos, sizeof(OperationType));
    pos += sizeof(OperationType);

    if (record.operation_type_ == OperationType::PUT ||
        record.operation_type_ == OperationType::DELETE) {
      uint16_t str_len;
      memcpy(&str_len, pos, sizeof(uint16_t));
      pos += sizeof(uint16_t);
      record.key_.assign(reinterpret_cast<const char *>(pos), str_len);
      pos += str_len;
    }
    if (record.operation_type_ == OperationType::PUT) {
      uint16_t str_len;
      memcpy(&str_len, pos, sizeof(uint16_t));
      pos += sizeof(uint16_t);
      record.value_.assign(reinterpret_cast<const char *>(pos), str_len);
      pos += str_len;
    }
    res.emplace_back(record);
  }

  return res;
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
} // namespace tiny_lsm
