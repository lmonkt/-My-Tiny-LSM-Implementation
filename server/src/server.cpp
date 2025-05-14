#include "../../include/redis_wrapper/redis_wrapper.h"
#include "../include/handler.h"
#include <cstddef>
#include <iostream>
#include <muduo/base/Logging.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/net/TcpServer.h>
#include <string>
#include <unordered_map>
#include <vector>

using namespace muduo;
using namespace muduo::net;

class RedisServer {
public:
  RedisServer(EventLoop *loop, const InetAddress &listenAddr)
      : server_(loop, listenAddr, "RedisServer"), redis("example_db") {
    server_.setConnectionCallback(
        std::bind(&RedisServer::onConnection, this, std::placeholders::_1));
    server_.setMessageCallback(
        std::bind(&RedisServer::onMessage, this, std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3));
  }

  void start() { server_.start(); }

  // private:
  void onConnection(const TcpConnectionPtr &conn) {
#ifdef LSM_DEBUG
    if (conn->connected()) {
      LOG_INFO << "Connection from " << conn->peerAddress().toIpPort();
    } else {
      LOG_INFO << "Connection closed from " << conn->peerAddress().toIpPort();
    }
#endif
  }

  void onMessage(const TcpConnectionPtr &conn, Buffer *buf, Timestamp time) {
    std::string msg(buf->retrieveAllAsString());
#ifdef LSM_DEBUG
    LOG_INFO << "Received message at " << time.toString() << ":\n" << msg;
#endif

    // 解析并处理请求
    std::string response = handleRequest(msg);
    conn->send(response);
  }

  std::string handleRequest(const std::string &request) {
    // TODO: Lab 6.6 处理网络传输的RESP字节流
    // TODO: Lab 6.6 形成参数并调用 redis_wrapper 的api
    // TODO: Lab 6.6 返回结果
    return "";
  }

  TcpServer server_;
  RedisWrapper redis; // 简单的键值存储
};

int main() {
  EventLoop loop;
  InetAddress listenAddr(6379); // Redis默认端口
  RedisServer server(&loop, listenAddr);

  server.start();
  loop.loop(); // 进入事件循环
  // DEBUG
  // server.handleRequest(
  //     "*3\r\n$3\r\nSET\r\n$6\r\npasswd\r\n$12\r\ntmdgnnwscjl\r\n");
}