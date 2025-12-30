#include <algorithm>
#include <asio.hpp>
#include <asio/ts/buffer.hpp>
#include <asio/ts/internet.hpp>
#include <cctype>
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

// 假设这些头文件和实现已经存在，并且与您的 Muduo 版本兼容
#include "../../include/redis_wrapper/redis_wrapper.h"
#include "../include/handler.h"

// 简单的日志宏，替代 Muduo 的 LOG_INFO
#define ASYNC_REDIS_SERVER_LOG_INFO(msg)                                       \
  std::cout << "[INFO] " << msg << std::endl;
#define ASYNC_REDIS_SERVER_LOG_DEBUG(                                          \
    msg) /* std::cout << "[DEBUG] " << msg << std::endl; */ // 在发布版本中可以注释掉

using asio::ip::tcp;

// 每个客户端连接的会话类
class RedisSession : public std::enable_shared_from_this<RedisSession> {
public:
  RedisSession(tcp::socket socket, RedisWrapper &redis_db)
      : socket_(std::move(socket)), redis_(redis_db) {}

  void start() {
    ASYNC_REDIS_SERVER_LOG_INFO(
        "Connection from " << socket_.remote_endpoint().address().to_string()
                           << ":" << socket_.remote_endpoint().port());
    do_read();
  }

private:
  void do_read() {
    auto self(shared_from_this());
    asio::async_read_until(
        socket_, buffer_, "\r\n",
        [this, self](const asio::error_code &ec,
                     std::size_t bytes_transferred) {
          if (!ec) {
            // 读取请求的完整内容，可能包含多行
            std::istream is(&buffer_);
            std::string request_line;
            std::getline(is, request_line); // 读取第一行，例如 "*3" 或 "PING"
            request_line += "\r\n"; // getline 会去除换行符，Redis协议需要

            // 对于PING，特殊处理，避免后续解析错误
            if (request_line == "PING\r\n") {
              std::string response = "+PONG\r\n";
              do_write(response);
              return; // PONG命令只有一行
            }

            // 尝试解析数组的元素数量
            if (request_line.length() < 2 || request_line[0] != '*') {
              do_write("-ERR Protocol error: expected '*'\r\n");
              return;
            }

            int numElements = 0;
            try {
              numElements = std::stoi(request_line.substr(1));
            } catch (const std::exception &e) {
              do_write("-ERR Protocol error: invalid number of elements\r\n");
              return;
            }

            // 继续读取剩余的批量字符串数据
            std::string full_request = request_line;
            for (int i = 0; i < numElements * 2;
                 ++i) { // 每个元素有两个部分：长度行和数据行
              if (buffer_.size() == 0) { // 需要更多数据
                // 确保缓冲区有足够的数据来读取完整的请求
                // 这是一种简化的处理方式，实际生产中需要更复杂的解析逻辑
                // 例如，可以分阶段读取，或者使用一个可增长的缓冲区
                do_write("-ERR Protocol error: partial request received\r\n");
                return;
              }
              std::string line;
              std::getline(is, line);
              full_request += line + "\r\n";
            }

            ASYNC_REDIS_SERVER_LOG_DEBUG("Received message:\n" << full_request);

            // 解析并处理请求
            std::string response = handleRequest(full_request);
            do_write(response);
          } else if (ec == asio::error::eof ||
                     ec == asio::error::connection_reset) {
            ASYNC_REDIS_SERVER_LOG_INFO(
                "Connection closed from "
                << socket_.remote_endpoint().address().to_string() << ":"
                << socket_.remote_endpoint().port());
          } else {
            ASYNC_REDIS_SERVER_LOG_INFO("Error on read: " << ec.message());
          }
        });
  }

  void do_write(const std::string &response) {
    auto self(shared_from_this());
    asio::async_write(
        socket_, asio::buffer(response),
        [this, self, response](const asio::error_code &ec,
                               std::size_t /*bytes_transferred*/) {
          if (!ec) {
            ASYNC_REDIS_SERVER_LOG_DEBUG("Sent response:\n" << response);
            do_read(); // 继续读取下一个请求
          } else if (ec == asio::error::eof ||
                     ec == asio::error::connection_reset) {
            ASYNC_REDIS_SERVER_LOG_INFO(
                "Connection closed from "
                << socket_.remote_endpoint().address().to_string() << ":"
                << socket_.remote_endpoint().port());
          } else {
            ASYNC_REDIS_SERVER_LOG_INFO("Error on write: " << ec.message());
          }
        });
  }

  // 与 Muduo 版本相同的 handleRequest 逻辑
  std::string handleRequest(const std::string &request) {
    // TODO: Lab 6.6 处理网络传输的RESP字节流
    // TODO: Lab 6.6 形成参数并调用 redis_wrapper 的api
    // TODO: Lab 6.6 返回结果
    if (request.empty())
      return "-ERR Protocol error: empty request\r\n";

    size_t cursor = 0;

    if (request[cursor] != '*')
      return "-ERR Protocol error: expected '*'\r\n";

    cursor++;

    size_t line_end = request.find("\r\n", cursor);
    if (line_end == std::string::npos)
      return "-ERR Protocol error: invalid request\r\n";

    int element_num = 0;
    try {
      element_num = std::stoi(request.substr(cursor, line_end - cursor));
    } catch (const std::exception &) {
      return "-ERR Protocol error: invalid number of elements\r\n";
    }

    cursor = line_end + 2;

    std::vector<std::string> args;
    args.reserve(element_num);

    for (int i = 0; i < element_num; ++i) {
      cursor = request.find('$', line_end);
      if (cursor >= request.size() || request[cursor] != '$') {
        return "-ERR Protocol error: expected '$'\r\n";
      }
      cursor++;

      line_end = request.find("\r\n", cursor);
      if (line_end == std::string::npos)
        return "-ERR Protocol error\r\n";

      int length = 0;
      try {
        length = std::stoi(request.substr(cursor, line_end - cursor));
      } catch (...) {
        return "-ERR Protocol error\r\n";
      }

      cursor = line_end + 2;

      if (cursor + length > request.size())
        return "-ERR Protocol error: incomplete\r\n";

      args.emplace_back(request.substr(cursor, length));

      line_end = cursor + length;
    }

    if (args.empty())
      return "-ERR empty command\r\n";
    // std::transform(args[0].begin(), args[0].end(), args[0].begin(),
    // ::toupper);

    std::string cmd = args[0];
    OPS op = string2Ops(cmd);

    switch (op) {
    // === 特殊操作 ===
    case OPS::PING:
      return "+PONG\r\n"; // 题目要求的特殊处理

    case OPS::UNKNOWN:
      return "-ERR unknown command '" + cmd + "'\r\n";

    // === IO 操作 ===
    case OPS::FLUSHALL:
      return flushall_handler(redis_);
    case OPS::SAVE:
      return save_handler(redis_);

    // === KV 基础操作 ===
    case OPS::GET:
      return get_handler(args, redis_);
    case OPS::SET:
      return set_handler(args, redis_);
    case OPS::DEL:
      return del_handler(args, redis_);
    case OPS::INCR:
      return incr_handler(args, redis_);
    case OPS::DECR:
      return decr_handler(args, redis_);
    case OPS::EXPIRE:
      return expire_handler(args, redis_);
    case OPS::TTL:
      return ttl_handler(args, redis_);

    // === Hash 操作 ===
    case OPS::HSET:
      return hset_handler(args, redis_);
    case OPS::HGET:
      return hget_handler(args, redis_);
    case OPS::HDEL:
      return hdel_handler(args, redis_);
    case OPS::HKEYS:
      return hkeys_handler(args, redis_);

    // === List 操作 ===
    case OPS::LPUSH:
      return lpush_handler(args, redis_);
    case OPS::RPUSH:
      return rpush_handler(args, redis_);
    case OPS::LPOP:
      return lpop_handler(args, redis_);
    case OPS::RPOP:
      return rpop_handler(args, redis_);
    case OPS::LLEN:
      return llen_handler(args, redis_);
    case OPS::LRANGE:
      return lrange_handler(args, redis_);

    // === ZSet 操作 ===
    case OPS::ZADD:
      return zadd_handler(args, redis_);
    case OPS::ZREM:
      return zrem_handler(args, redis_);
    case OPS::ZRANGE:
      return zrange_handler(args, redis_);
    case OPS::ZCARD:
      return zcard_handler(args, redis_);
    case OPS::ZSCORE:
      return zscore_handler(args, redis_);
    case OPS::ZINCRBY:
      return zincrby_handler(args, redis_);
    case OPS::ZRANK:
      return zrank_handler(args, redis_);

    // === Set 操作 ===
    case OPS::SADD:
      return sadd_handler(args, redis_);
    case OPS::SREM:
      return srem_handler(args, redis_);
    case OPS::SISMEMBER:
      return sismember_handler(args, redis_);
    case OPS::SCARD:
      return scard_handler(args, redis_);
    case OPS::SMEMBERS:
      return smembers_handler(args, redis_);

    default:
      return "-ERR unimplemented command\r\n";
    }
  }

  tcp::socket socket_;
  asio::streambuf buffer_; // 使用 streambuf 处理读入的数据
  RedisWrapper &redis_;    // 数据库实例，通过引用传递
};

class RedisServer {
public:
  RedisServer(asio::io_context &io_context, unsigned short port)
      : acceptor_(io_context, tcp::endpoint(tcp::v4(), port)),
        redis_("example_db") { // 初始化 RedisWrapper
    do_accept();
  }

private:
  void do_accept() {
    acceptor_.async_accept(
        [this](const asio::error_code &ec, tcp::socket socket) {
          if (!ec) {
            std::make_shared<RedisSession>(std::move(socket), redis_)->start();
          } else {
            ASYNC_REDIS_SERVER_LOG_INFO("Error on accept: " << ec.message());
          }
          do_accept(); // 继续接受新的连接
        });
  }

  tcp::acceptor acceptor_;
  RedisWrapper redis_; // 数据库实例
};

int main() {
  try {
    asio::io_context io_context;
    RedisServer server(io_context, 6379); // Redis 默认端口
    io_context.run();                     // 运行事件循环
  } catch (std::exception &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
  }

  return 0;
}
