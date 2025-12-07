#include "../../include/redis_wrapper/redis_wrapper.h"
#include "../../include/config/config.h"
#include "../../include/consts.h"
#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tiny_lsm {

// Helper functions
std::vector<std::string>
get_fileds_from_hash_value(const std::optional<std::string> &field_list_opt) {
  std::vector<std::string> res;
  if (!field_list_opt.has_value())
    return res;

  const std::string &s = *field_list_opt;
  if (s.empty())
    return res;

  char sep = TomlConfig::getInstance().getRedisFieldSeparator();
  std::string cur;
  for (char c : s) {
    if (c == sep) {
      res.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  if (!cur.empty())
    res.push_back(cur);
  return res;
}

/*
 * 函数: get_fileds_from_hash_value
 * 说明:
 *   将哈希类型的 meta 值（即以定界符拼接的字段名字字符串）解析成字段名向量。
 *   例如: 如果 meta 存储为 "field1|field2|field3"（假设分隔符为 '|'），
 *   则返回 {"field1", "field2", "field3"}。
 *
 * 参数:
 *   - field_list_opt: 可选的字符串，表示 meta 中字段名的拼接字符串；
 *                     如果 std::optional 没有值或字符串为空，返回空 vector。
 *
 * 返回值:
 *   - std::vector<std::string>：解析后的字段名列表，顺序与原 meta
 * 字符串中一致。
 *
 * 注意:
 *   - 分隔符由 TomlConfig::getInstance().getRedisFieldSeparator() 获取；
 *   - 该函数不会去除字段名前后的空白，调用者若需要应当自行处理；
 */

std::string get_hash_value_from_fields(const std::vector<std::string> &fields) {
  char sep = TomlConfig::getInstance().getRedisFieldSeparator();
  std::string out;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (i)
      out.push_back(sep);
    out += fields[i];
  }
  return out;
}

/*
 * 函数: get_hash_value_from_fields
 * 说明:
 *   将字段名向量按配置中的分隔符拼接成一个字符串，作为哈希 meta 的存储值。
 *   例如: 输入 {"field1","field2"}，分隔符为 '|' 时返回 "field1|field2"。
 *
 * 参数:
 *   - fields: 字段名列表，返回字符串中字段顺序与该列表一致。
 *
 * 返回值:
 *   - std::string：拼接后的字段名字符串；当 fields 为空时返回空字符串。
 *
 * 注意:
 *   - 拼接分隔符由 TomlConfig
 * 获取；该函数仅做字符串拼接，不去重也不做顺序保证之外的任何变换。
 */

inline std::string get_hash_filed_key(const std::string &key,
                                      const std::string &field) {
  const std::string &prefix = TomlConfig::getInstance().getRedisFieldPrefix();
  char sep = TomlConfig::getInstance().getRedisFieldSeparator();
  return prefix + key + sep + field;
}

/*
 * 函数: get_hash_filed_key
 * 说明:
 *   根据给定的哈希键（key）和字段（field），构造该字段在底层存储中的完整键名。
 *   通常哈希类型在底层实现为：每个字段单独存储一条 key-value，
 *   该函数用来生成字段级别的存储 key。
 *
 * 参数:
 *   - key: 哈希类型的主键名（例如 redis 中的哈希 key）；
 *   - field: 字段名（hash field）；
 *
 * 返回值:
 *   - std::string：由配置的 field 前缀 + 主键 + 分隔符 + 字段组成的完整存储
 * key。
 *
 * 举例:
 *   如果 redis field prefix 为 "f_"，field separator 为 ':'，
 *   key='myhash'，field='name'，则结果为 "f_myhash:name"。
 */

inline std::string get_hash_meta_key(const std::string &key) {
  return TomlConfig::getInstance().getRedisHashValuePreffix() + key;
}

/*
 * 函数: get_hash_meta_key
 * 说明:
 *   获取哈希类型的 meta 键（用于保存该哈希所有字段名的拼接字符串），
 *   这个 key 与字段存储 key 不同，通常只存储字段名的列表（meta 信息）。
 *
 * 参数:
 *   - key: 哈希类型的主键名；
 *
 * 返回值:
 *   - std::string：配置中定义的 hash meta 前缀 + key。
 *
 * 举例:
 *   如果 redis hash meta prefix 为 "h_"，key='myhash'，则返回 "h_myhash"。
 */

inline bool is_value_hash(const std::string &key) {
  const std::string &prefix =
      TomlConfig::getInstance().getRedisHashValuePreffix();
  if (key.rfind(prefix, 0) == 0)
    return true;
  return false;
}

/*
 * 函数: is_value_hash
 * 说明:
 *   判断给定 key 是否带有 hash meta 的前缀（用于识别是否为哈希的 meta key），
 *   这里通过前缀匹配来判断：如果 key 以 HashValuePreffix 开头则返回 true。
 *
 * 参数:
 *   - key: 待判断的 key；
 *
 * 返回值:
 *   - bool：true 表示该 key 看起来像是哈希类型的 meta key，false 表示不是。
 *
 * 注意:
 *   - 该函数只在语义上帮助判断，不保证该 key 在数据库中实际存在。
 */

inline std::string get_explire_key(const std::string &key) {
  return TomlConfig::getInstance().getRedisExpireHeader() + key;
}

/*
 * 函数: get_explire_key
 * 说明:
 *   为给定 key 构造对应的过期时间存储 key，通常通过在原 key
 * 前加一个过期前缀实现， 这样过期时间可以与原 key 分开存储（例如 value 存储在
 * key，而绝对过期时间存储在 expire_key）。
 *
 * 参数:
 *   - key: 待生成过期 key 的原始 key；
 *
 * 返回值:
 *   - std::string: 配置中定义的过期 header + key。
 *
 * 举例:
 *   如果 RedisExpireHeader 是 "expire_"，key = "foo"，则返回 "expire_foo"。
 */
RedisWrapper::RedisWrapper(const std::string &db_path) {
  this->lsm = std::make_unique<LSM>(db_path);
}

// ************************ Redis Helper Func *************************

// ************************* Redis Command *************************
// 基础操作
std::string RedisWrapper::set(std::vector<std::string> &args) {
  return redis_set(args[1], args[2]);
}
std::string RedisWrapper::get(std::vector<std::string> &args) {
  return redis_get(args[1]);
}
std::string RedisWrapper::del(std::vector<std::string> &args) {
  return redis_del(args);
}
std::string RedisWrapper::incr(std::vector<std::string> &args) {
  return redis_incr(args[1]);
}

std::string RedisWrapper::decr(std::vector<std::string> &args) {
  return redis_decr(args[1]);
}

std::string RedisWrapper::expire(std::vector<std::string> &args) {
  return redis_expire(args[1], args[2]);
}

std::string RedisWrapper::ttl(std::vector<std::string> &args) {
  return redis_ttl(args[1]);
}

// 哈希操作
std::string RedisWrapper::hset(std::vector<std::string> &args) {
  // return redis_hset(args[1], args[2], args[3]);
  if (args.size() < 4 || (args.size() - 1) % 2 != 1) {
    return "-ERR wrong number of arguments for 'hset' command\r\n";
  }

  const std::string &key = args[1];
  std::vector<std::pair<std::string, std::string>> fieldValues;

  // 从第2个参数开始，每两个为一组 field-value
  for (size_t i = 2; i < args.size(); i += 2) {
    if (i + 1 >= args.size())
      break; // 防止越界
    fieldValues.emplace_back(args[i], args[i + 1]);
  }
  return redis_hset_batch(key, fieldValues);
}

std::string RedisWrapper::hget(std::vector<std::string> &args) {
  return redis_hget(args[1], args[2]);
}

std::string RedisWrapper::hdel(std::vector<std::string> &args) {
  return redis_hdel(args[1], args[2]);
}

std::string RedisWrapper::hkeys(std::vector<std::string> &args) {
  return redis_hkeys(args[1]);
}

// 链表操作
std::string RedisWrapper::lpush(std::vector<std::string> &args) {
  return redis_lpush(args[1], args[2]);
}
std::string RedisWrapper::rpush(std::vector<std::string> &args) {
  return redis_rpush(args[1], args[2]);
}
std::string RedisWrapper::lpop(std::vector<std::string> &args) {
  return redis_lpop(args[1]);
}
std::string RedisWrapper::rpop(std::vector<std::string> &args) {
  return redis_rpop(args[1]);
}
std::string RedisWrapper::llen(std::vector<std::string> &args) {
  return redis_llen(args[1]);
}
std::string RedisWrapper::lrange(std::vector<std::string> &args) {
  int start = std::stoi(args[2]);
  int end = std::stoi(args[3]);

  return redis_lrange(args[1], start, end);
}

// 有序集合操作
std::string RedisWrapper::zadd(std::vector<std::string> &args) {
  return redis_zadd(args);
}

std::string RedisWrapper::zrem(std::vector<std::string> &args) {
  return redis_zrem(args);
}

std::string RedisWrapper::zrange(std::vector<std::string> &args) {
  return redis_zrange(args);
}

std::string RedisWrapper::zcard(std::vector<std::string> &args) {

  return redis_zcard(args[1]);
}

std::string RedisWrapper::zscore(std::vector<std::string> &args) {
  return redis_zscore(args[1], args[2]);
}
std::string RedisWrapper::zincrby(std::vector<std::string> &args) {
  return redis_zincrby(args[1], args[2], args[3]);
}

std::string RedisWrapper::zrank(std::vector<std::string> &args) {
  return redis_zrank(args[1], args[2]);
}

// 无序集合操作
std::string RedisWrapper::sadd(std::vector<std::string> &args) {
  return redis_sadd(args);
}

std::string RedisWrapper::srem(std::vector<std::string> &args) {
  return redis_srem(args);
}

std::string RedisWrapper::sismember(std::vector<std::string> &args) {
  return redis_sismember(args[1], args[2]);
}

std::string RedisWrapper::scard(std::vector<std::string> &args) {
  return redis_scard(args[1]);
}

std::string RedisWrapper::smembers(std::vector<std::string> &args) {
  return redis_smembers(args[1]);
}

void RedisWrapper::clear() { this->lsm->clear(); }
void RedisWrapper::flushall() { this->lsm->flush(); }

// *********************** Redis ***********************
// 基础操作
std::string RedisWrapper::redis_incr(const std::string &key) {
  // TODO: Lab 6.1 自增一个值类型的key
  // ? 不存在则新建一个值为1的key
  std::string mutable_key = key;
  auto res = redis_get(mutable_key);
  // If key not found (null bulk string) or TTL returned an error on get
  if (res == "$-1\r\n" || res == "-ERR invalid expire time format\r\n") {
    std::string mutable_value = "1";
    redis_set(mutable_key, mutable_value);
    return mutable_value;
  }

  // Parse RESP bulk string "$<len>\r\n<value>\r\n"
  auto pos = res.find("\r\n");
  if (pos == std::string::npos || res.size() < 3 || res[0] != '$') {
    return "-ERR syntax error\r\n";
  }
  std::string len_tmp = res.substr(1, pos - 1);
  int length = 0;
  try {
    length = std::stoi(len_tmp);
  } catch (...) {
    return "-ERR syntax error\r\n";
  }
  size_t start = pos + 2; // after \r\n
  if (res.size() < start + (size_t)length) {
    return "-ERR syntax error\r\n";
  }
  auto value = res.substr(start, length);

  // Validate numeric string (allow leading negative sign)
  try {
    long long num_value = std::stoll(value);
    num_value += 1;
    auto v = std::to_string(num_value);
    redis_set(mutable_key, v);
    return v;
  } catch (...) {
    return "-ERR syntax error\r\n";
  }
}

std::string RedisWrapper::redis_decr(const std::string &key) {
  // Lab 6.1: decrement a numeric value stored at key
  std::string mutable_key = key;
  auto res = redis_get(mutable_key);
  // If key not found (null bulk string) or TTL returned an error on get
  if (res == "$-1\r\n" || res == "-ERR invalid expire time format\r\n") {
    std::string mutable_value = "-1";
    redis_set(mutable_key, mutable_value);
    return mutable_value;
  }

  // Parse RESP bulk string "$<len>\r\n<value>\r\n"
  auto pos = res.find("\r\n");
  if (pos == std::string::npos || res.size() < 3 || res[0] != '$') {
    return "-ERR syntax error\r\n";
  }
  std::string len_tmp = res.substr(1, pos - 1);
  int length = 0;
  try {
    length = std::stoi(len_tmp);
  } catch (...) {
    return "-ERR syntax error\r\n";
  }
  size_t start = pos + 2; // after \r\n
  if (res.size() < start + (size_t)length) {
    return "-ERR syntax error\r\n";
  }
  auto value = res.substr(start, length);

  // Validate and parse numeric string (allow leading negative sign)
  try {
    long long num_value = std::stoll(value);
    num_value -= 1;
    auto v = std::to_string(num_value);
    redis_set(mutable_key, v);
    return v;
  } catch (...) {
    return "-ERR syntax error\r\n";
  }
}

std::string RedisWrapper::redis_del(std::vector<std::string> &args) {
  // Lab 6.1 删除一个或多个 key（遵循 Redis DEL 支持多个 key 的语义）
  // Redis DEL 返回被删除 key 的数量 (整型)，格式为 ":<count>\r\n"。
  int del_count = 0;

  // 收集要删除的真正 key（包括可能的 expire_ 前缀项），然后批量删除以提高性能
  std::vector<std::string> to_remove;
  for (size_t i = 1; i < args.size(); ++i) {
    const std::string &k = args[i];
    std::string explire_key = get_explire_key(k);
    // 如果 key 存在，则把它计数并标记删除（同时删除它对应的过期键）
    if (lsm->get(k)) {
      ++del_count;
      to_remove.push_back(k);
      to_remove.push_back(explire_key);
    } else {
      // 如果基本键不存在但存在 expier_* 记录，可以一并删除，但不计入 del_count
      if (lsm->get(explire_key)) {
        to_remove.push_back(explire_key);
      }
    }
  }

  if (!to_remove.empty()) {
    lsm->remove_batch(to_remove);
  }

  return ":" + std::to_string(del_count) + "\r\n";
}

std::string RedisWrapper::redis_expire(const std::string &key,
                                       std::string seconds_count) {
  // TODO: Lab 6.1 设置一个`key`的过期时间
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  // 转换时间
  long seconds;
  try {
    seconds = std::stol(seconds_count);
  } catch (...) {
    return "-ERR invalid expire time\r\n";
  }

  time_t expire_time = std::time(nullptr) + seconds;
  std::string expire_val = std::to_string(expire_time);

  // 1. 检查是否存在对应的 Hash Meta Key (优先检查
  // Hash，或者根据你的系统设计顺序)
  std::string metaKey = get_hash_meta_key(key);
  if (auto value = lsm->get(metaKey)) {
    // 关键点：如果是 Hash，必须将过期时间设置在 metaKey 对应的 expireKey 上
    // 这样 redis_hset_batch 中的检查逻辑才能读到
    lsm->put(get_explire_key(metaKey), expire_val);
    return ":1\r\n";
  }

  // 2. 检查是否存在普通的 Key (String 类型)
  // 如果你的系统不仅仅支持 Hash，还支持 String，需要保留这个检查
  if (auto value = lsm->get(key)) {
    lsm->put(get_explire_key(key), expire_val);
    return ":1\r\n";
  }

  // 3. 键不存在
  return ":0\r\n";
}

std::string RedisWrapper::redis_set(std::string &key, std::string &value) {
  // TODO: Lab 6.1 新建(或更改)一个`key`的值
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  lsm->put(key, value);
  return "+OK\r\n";
}

std::string RedisWrapper::redis_get(std::string &key) {
  // TODO: Lab 6.1 获取一个`key`的值
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  auto expire_value = lsm->get(get_explire_key(key));
  if (expire_value) {
    try {
      time_t current_time = std::time(nullptr);       // 获取当前时间
      time_t expire_time = std::stoll(*expire_value); // 转换为整数时间戳

      // 2. 判断是否过期
      if (expire_time <= current_time) {
        // 3. 删除过期键
        lsm->remove(key);
        lsm->remove(get_explire_key(key));
        // 按键不存在处理
        return "$-1\r\n"; // RESP: null bulk string
      }
    } catch (const std::exception &e) {
      // 处理转换错误（可选）
      return "-ERR invalid expire time format\r\n";
    }
  }

  // 4. 检查键是否存在
  auto value = lsm->get(key);
  if (!value) {
    return "$-1\r\n"; // RESP: null bulk string
  }

  // 5. 返回正常值（RESP bulk string 格式）
  std::string value_str = *value;
  return "$" + std::to_string(value_str.size()) + "\r\n" + value_str + "\r\n";
}

std::string RedisWrapper::redis_ttl(std::string &key) {
  // TODO: Lab 6.1 获取一个`key`的剩余过期时间
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  auto value = lsm->get(key);
  if (!value) {
    return ":-2\r\n"; // key does not exist
  }
  auto expire_value = lsm->get(get_explire_key(key));
  if (!expire_value)
    return ":-1\r\n";
  try {
    time_t now = std::time(nullptr);
    time_t expire_time = std::stoll(*expire_value);
    long remain = static_cast<long>(expire_time - now);
    if (remain <= 0) {
      lsm->remove(key);
      lsm->remove(get_explire_key(key));
      return ":-2\r\n";
    }
    return ":" + std::to_string(remain) + "\r\n";
  } catch (...) {
    return "-ERR invalid expire time format\r\n";
  }
}

// 哈希操作
std::string RedisWrapper::redis_hset_batch(
    const std::string &key,
    std::vector<std::pair<std::string, std::string>> &field_value_pairs) {
  // TODO: Lab 6.2 批量设置一个哈希类型的`key`的多个字段值
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  // We need exclusive lock for writes to the hash meta + fields
  std::unique_lock<std::shared_mutex> wlock(redis_mtx);
  int added_count = 0;

  std::string metaKey = get_hash_meta_key(key);
  std::string expireKey = get_explire_key(metaKey);

  // 1) TTL check and cleanup if expired
  if (auto expire_value = lsm->get(expireKey)) {
    try {
      time_t current_time = std::time(nullptr);
      time_t expire_time = std::stoll(*expire_value);
      if (expire_time <= current_time) {
        // need to remove metaKey, expireKey and all fields
        if (auto meta_val = lsm->get(metaKey)) {
          auto fields = get_fileds_from_hash_value(meta_val);
          std::vector<std::string> remove_keys;
          for (auto &f : fields) {
            remove_keys.push_back(get_hash_filed_key(key, f));
          }
          remove_keys.push_back(metaKey);
          remove_keys.push_back(expireKey);
          if (!remove_keys.empty())
            lsm->remove_batch(remove_keys);
        }
      }
    } catch (...) {
      return "-ERR invalid expire time format\r\n";
    }
  }

  // 2) Determine which fields already exist using batch get
  std::vector<std::string> fieldKeys;
  fieldKeys.reserve(field_value_pairs.size());
  for (auto &p : field_value_pairs) {
    fieldKeys.push_back(get_hash_filed_key(key, p.first));
  }

  auto existing_field_results = lsm->get_batch(fieldKeys);

  // 3) Prepare put_batch vector and update meta
  std::vector<std::pair<std::string, std::string>> to_put;
  to_put.reserve(field_value_pairs.size() + 1);

  // Determine existing fields map
  std::unordered_set<std::string> existing_fields;
  for (size_t i = 0; i < existing_field_results.size(); ++i) {
    if (existing_field_results[i].second.has_value()) {
      // existing => count not increased
      existing_fields.insert(field_value_pairs[i].first);
    }
  }

  // push all field kvs
  for (auto &p : field_value_pairs) {
    to_put.emplace_back(get_hash_filed_key(key, p.first), p.second);
    if (existing_fields.find(p.first) == existing_fields.end()) {
      ++added_count; // newly added
    }
  }

  // update metaKey
  std::vector<std::string> meta_fields;
  if (auto meta_val = lsm->get(metaKey)) {
    meta_fields = get_fileds_from_hash_value(meta_val);
  }
  // preserve order: existing meta fields order + append new unique fields in
  // same order
  std::vector<std::string> merged_vec = meta_fields;
  std::unordered_set<std::string> exist_set(meta_fields.begin(),
                                            meta_fields.end());
  for (auto &p : field_value_pairs) {
    if (exist_set.insert(p.first).second)
      merged_vec.push_back(p.first);
  }
  to_put.emplace_back(metaKey, get_hash_value_from_fields(merged_vec));

  // 4) write in batch
  if (!to_put.empty()) {
    lsm->put_batch(to_put);
  }

  return ":" + std::to_string(added_count) + "\r\n";
}

std::string RedisWrapper::redis_hset(const std::string &key,
                                     const std::string &field,
                                     const std::string &value) {
  // TODO: Lab 6.2 设置一个哈希类型的`key`的某个字段值
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  // We need exclusive lock for writes to the hash meta & field keys
  std::unique_lock<std::shared_mutex> wlock(redis_mtx);

  std::string metaKey = get_hash_meta_key(key);
  std::string expireKey = get_explire_key(metaKey);

  // 1) TTL check and cleanup if expired
  if (auto expire_value = lsm->get(expireKey)) {
    try {
      time_t current_time = std::time(nullptr);
      time_t expire_time = std::stoll(*expire_value);
      if (expire_time <= current_time) {
        // need to remove metaKey, expireKey and all fields
        if (auto meta_val = lsm->get(metaKey)) {
          auto fields = get_fileds_from_hash_value(meta_val);
          std::vector<std::string> remove_keys;
          for (auto &f : fields) {
            remove_keys.push_back(get_hash_filed_key(key, f));
          }
          remove_keys.push_back(metaKey);
          remove_keys.push_back(expireKey);
          if (!remove_keys.empty())
            lsm->remove_batch(remove_keys);
        }
      }
    } catch (...) {
      return "-ERR invalid expire time format\r\n";
    }
  }

  std::string fieldKey = get_hash_filed_key(key, field);
  // If field exists already?
  auto field_val = lsm->get(fieldKey);
  bool existed = field_val.has_value();

  // We will write both the field and updated meta in a batch
  std::vector<std::pair<std::string, std::string>> kvs;
  kvs.emplace_back(fieldKey, value);

  // Update meta list
  std::vector<std::string> meta_fields;
  if (auto meta_val = lsm->get(metaKey)) {
    meta_fields = get_fileds_from_hash_value(meta_val);
  }
  std::vector<std::string> merged_vec = meta_fields;
  std::unordered_set<std::string> exist_set(meta_fields.begin(),
                                            meta_fields.end());
  if (exist_set.insert(field).second)
    merged_vec.push_back(field);
  kvs.emplace_back(metaKey, get_hash_value_from_fields(merged_vec));

  lsm->put_batch(kvs);

  return existed ? ":0\r\n" : ":1\r\n";
}

std::string RedisWrapper::redis_hget(const std::string &key,
                                     const std::string &field) {
  // TODO: Lab 6.2 获取一个哈希类型的`key`的某个字段值
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  std::shared_lock<std::shared_mutex> rlock(redis_mtx);
  std::string metaKey = get_hash_meta_key(key);
  std::string expireKey = get_explire_key(metaKey);

  // 1) TTL check and cleanup if expired
  if (auto expire_value = lsm->get(expireKey)) {
    try {
      time_t current_time = std::time(nullptr);
      time_t expire_time = std::stoll(*expire_value);
      if (expire_time <= current_time) {
        // need to remove metaKey, expireKey and all fields
        if (auto meta_val = lsm->get(metaKey)) {
          auto fields = get_fileds_from_hash_value(meta_val);
          std::vector<std::string> remove_keys;
          for (auto &f : fields) {
            remove_keys.push_back(get_hash_filed_key(key, f));
          }
          remove_keys.push_back(metaKey);
          remove_keys.push_back(expireKey);
          if (!remove_keys.empty())
            lsm->remove_batch(remove_keys);
        }
      }
      return "$-1\r\n";
    } catch (...) {
      return "-ERR invalid expire time format\r\n";
    }
  }

  std::string fieldKey = get_hash_filed_key(key, field);
  auto field_val = lsm->get(fieldKey);
  if (!field_val) {
    return "$-1\r\n";
  }
  std::string value_str = *field_val;
  return "$" + std::to_string(value_str.size()) + "\r\n" + value_str + "\r\n";
}

std::string RedisWrapper::redis_hdel(const std::string &key,
                                     const std::string &field) {
  // TODO: Lab 6.2 删除一个哈希类型的`key`的某个字段
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  auto get_res = redis_hget(key, field);
  if (get_res == "$-1\r\n")
    return ":0\r\n";

  std::unique_lock<std::shared_mutex> wlock(redis_mtx);
  std::string metaKey = get_hash_meta_key(key);
  std::string fieldKey = get_hash_filed_key(key, field);
  lsm->remove(fieldKey);

  return ":1\r\n";
}

std::string RedisWrapper::redis_hkeys(const std::string &key) {
  // TODO: Lab 6.2 获取一个哈希类型的`key`的所有字段
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM

  std::shared_lock<std::shared_mutex> rlock(redis_mtx);
  std::string metaKey = get_hash_meta_key(key);
  std::string expireKey = get_explire_key(metaKey);

  // 1) TTL check and cleanup if expired
  if (auto expire_value = lsm->get(expireKey)) {
    try {
      time_t current_time = std::time(nullptr);
      time_t expire_time = std::stoll(*expire_value);
      if (expire_time <= current_time) {
        // need to remove metaKey, expireKey and all fields
        if (auto meta_val = lsm->get(metaKey)) {
          auto fields = get_fileds_from_hash_value(meta_val);
          std::vector<std::string> remove_keys;
          for (auto &f : fields) {
            remove_keys.push_back(get_hash_filed_key(key, f));
          }
          remove_keys.push_back(metaKey);
          remove_keys.push_back(expireKey);
          if (!remove_keys.empty())
            lsm->remove_batch(remove_keys);
        }
      }
      return "*0\r\n";
    } catch (...) {
      return "-ERR invalid expire time format\r\n";
    }
  }

  std::vector<std::string> meta_fields;
  if (auto meta_val = lsm->get(metaKey)) {
    meta_fields = get_fileds_from_hash_value(meta_val);
    std::string res = "*" + std::to_string(meta_fields.size());
    for (auto field : meta_fields) {
      res += ("\r\n$" + std::to_string(field.size()) + "\r\n" + field);
    }
    return res + "\r\n";
  } else {
    return "*0\r\n";
  }
}

// 链表操作
std::string RedisWrapper::redis_lpush(const std::string &key,
                                      const std::string &value) {
  // TODO: Lab 6.5 新建一个链表类型的`key`，并添加一个元素到链表头部
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return ":" + std::to_string(1) + "\r\n";
}

std::string RedisWrapper::redis_rpush(const std::string &key,
                                      const std::string &value) {
  // TODO: Lab 6.5 新建一个链表类型的`key`，并添加一个元素到链表尾部
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return ":" + std::to_string(1) + "\r\n";
}

std::string RedisWrapper::redis_lpop(const std::string &key) {
  // TODO: Lab 6.5 获取一个链表类型的`key`的头部元素
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return "$-1\r\n"; // 表示链表不存在
}

std::string RedisWrapper::redis_rpop(const std::string &key) {
  // TODO: Lab 6.5 获取一个链表类型的`key`的尾部元素
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return "$-1\r\n"; // 表示链表不存在
}

std::string RedisWrapper::redis_llen(const std::string &key) {
  // TODO: Lab 6.5 获取一个链表类型的`key`的长度
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return ":1\r\n"; // 表示链表不存在
}

std::string RedisWrapper::redis_lrange(const std::string &key, int start,
                                       int stop) {
  // TODO: Lab 6.5 获取一个链表类型的`key`的指定范围内的元素
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return "*0\r\n"; // 表示链表不存在或者范围无效
}

std::string RedisWrapper::redis_zadd(std::vector<std::string> &args) {
  // TODO: Lab 6.4 如果有序集合不存在则新建，添加一个元素到有序集合中
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return ":1\r\n";
}

std::string RedisWrapper::redis_zrem(std::vector<std::string> &args) {
  // TODO: Lab 6.4 删除有序集合中的元素
  int removed_count = 0;
  return ":" + std::to_string(removed_count) + "\r\n";
}

std::string RedisWrapper::redis_zrange(std::vector<std::string> &args) {
  // TODO: Lab 6.4 获取有序集合中指定范围内的元素
  return "*0\r\n";
}

std::string RedisWrapper::redis_zcard(const std::string &key) {
  // TODO: Lab 6.4 获取有序集合的元素个数
  return ":1\r\n";
}

std::string RedisWrapper::redis_zscore(const std::string &key,
                                       const std::string &elem) {
  // TODO: Lab 6.4 获取有序集合中元素的分数
  return "$-1\r\n";
}

std::string RedisWrapper::redis_zincrby(const std::string &key,
                                        const std::string &increment,
                                        const std::string &elem) {
  // TODO: Lab 6.4 对有序集合中元素的分数进行增加
  return "$-1\r\n";
}

std::string RedisWrapper::redis_zrank(const std::string &key,
                                      const std::string &elem) {
  //  TODO: Lab 6.4 获取有序集合中元素的排名
  return "$-1\r\n";
}

std::string RedisWrapper::redis_sadd(std::vector<std::string> &args) {
  // TODO: Lab 6.3 如果集合不存在则新建，添加一个元素到集合中
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return ":1\r\n";
}

std::string RedisWrapper::redis_srem(std::vector<std::string> &args) {
  // TODO: Lab 6.3 删除集合中的元素
  int removed_count = 0;
  return ":" + std::to_string(removed_count) + "\r\n";
}

std::string RedisWrapper::redis_sismember(const std::string &key,
                                          const std::string &member) {
  // TODO: Lab 6.3 判断集合中是否存在某个元素
  return ":1\r\n";
}

std::string RedisWrapper::redis_scard(const std::string &key) {
  // TODO: Lab 6.3 获取集合的元素个数
  return ":1\r\n";
}

std::string RedisWrapper::redis_smembers(const std::string &key) {
  // TODO: Lab 6.3 获取集合的所有元素
  return "*0\r\n";
}
} // namespace tiny_lsm
