#include "../../include/redis_wrapper/redis_wrapper.h"
#include "../../include/config/config.h"
#include "../../include/consts.h"
#include <algorithm>
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

namespace toni_lsm {

// Helper functions
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
  return "1";
}

std::string RedisWrapper::redis_decr(const std::string &key) {
  // TODO: Lab 6.1 自增一个值类型的key
  // ? 不存在则新建一个值为-1的key
  return "-1";
}

std::string RedisWrapper::redis_del(std::vector<std::string> &args) {
  // TODO: Lab 6.1 删除一个key
  int del_count = 0;
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return ":" + std::to_string(del_count) + "\r\n";
}

std::string RedisWrapper::redis_expire(const std::string &key,
                                       std::string seconds_count) {
  // TODO: Lab 6.1 设置一个`key`的过期时间
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return ":1\r\n";
}

std::string RedisWrapper::redis_set(std::string &key, std::string &value) {
  // TODO: Lab 6.1 新建(或更改)一个`key`的值
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return "+OK\r\n";
}

std::string RedisWrapper::redis_get(std::string &key) {
  // TODO: Lab 6.1 获取一个`key`的值
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return "$-1\r\n"; // 表示键不存在
}

std::string RedisWrapper::redis_ttl(std::string &key) {
  // TODO: Lab 6.1 获取一个`key`的剩余过期时间
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return ":1\r\n"; // 表示键不存在
}

// 哈希操作
std::string RedisWrapper::redis_hset_batch(
    const std::string &key,
    std::vector<std::pair<std::string, std::string>> &field_value_pairs) {
  std::shared_lock<std::shared_mutex> rlock(redis_mtx);
  // TODO: Lab 6.2 批量设置一个哈希类型的`key`的多个字段值
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  int added_count = 0;
  return ":" + std::to_string(added_count) + "\r\n";
}
std::string RedisWrapper::redis_hset(const std::string &key,
                                     const std::string &field,
                                     const std::string &value) {
  std::shared_lock<std::shared_mutex> rlock(redis_mtx); // 读锁线判断是否过期
  // TODO: Lab 6.2 设置一个哈希类型的`key`的某个字段值
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return "+OK\r\n";
}

std::string RedisWrapper::redis_hget(const std::string &key,
                                     const std::string &field) {
  // TODO: Lab 6.2 获取一个哈希类型的`key`的某个字段值
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return "$-1\r\n"; // 表示键不存在
}

std::string RedisWrapper::redis_hdel(const std::string &key,
                                     const std::string &field) {
  // TODO: Lab 6.2 删除一个哈希类型的`key`的某个字段
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return ":1\r\n";
}

std::string RedisWrapper::redis_hkeys(const std::string &key) {
  // TODO: Lab 6.2 获取一个哈希类型的`key`的所有字段
  // ? 返回值的格式, 你需要查询 RESP 官方文档或者问 LLM
  return "*0\r\n";
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
} // namespace toni_lsm