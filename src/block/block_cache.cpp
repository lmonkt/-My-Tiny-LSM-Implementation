#include "../../include/block/block_cache.h"
#include "../../include/block/block.h"
#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace tiny_lsm {
BlockCache::BlockCache(size_t capacity, size_t k)
    : capacity_(capacity), k_(k) {}

BlockCache::~BlockCache() = default;

std::shared_ptr<Block> BlockCache::get(int sst_id, int block_id) {
  // TODO: Lab 4.8 查询一个 Block
  std::lock_guard<std::mutex> lock(mutex_);
  ++total_requests_; // 增加总请求数
  auto key = std::make_pair(sst_id, block_id);
  auto it = cache_map_.find(key);
  if (it == cache_map_.end()) {
    return nullptr;
  }
  ++hit_requests_;
  update_access_count(it->second);
  return it->second->cache_block;
}

void BlockCache::put(int sst_id, int block_id, std::shared_ptr<Block> block) {
  // TODO: Lab 4.8 插入一个 Block
  std::lock_guard<std::mutex> lock(mutex_);
  auto key = std::make_pair(sst_id, block_id);
  auto it = cache_map_.find(key);
  if (it != cache_map_.end()) {
    it->second->cache_block = block;
    update_access_count(it->second);
  } else {
    if (cache_map_.size() >= capacity_) {
      // 优先淘汰 less_k 中最久未访问的（LRU）
      if (!cache_list_less_k.empty()) {
        cache_map_.erase(std::make_pair(cache_list_less_k.back().sst_id,
                                        cache_list_less_k.back().block_id));
        cache_list_less_k.pop_back();
      }
      // 若 less_k 为空，淘汰 greater_k 尾部（最久未访问的高频项）
      else {
        cache_map_.erase(std::make_pair(cache_list_greater_k.back().sst_id,
                                        cache_list_greater_k.back().block_id));
        cache_list_greater_k.pop_back();
      }
    }
    cache_list_less_k.push_front({sst_id, block_id, block, 1});
    cache_map_[key] = cache_list_less_k.begin();
  }
}

double BlockCache::hit_rate() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return total_requests_ == 0
             ? 0.0
             : static_cast<double>(hit_requests_) / total_requests_;
}

void BlockCache::update_access_count(std::list<CacheItem>::iterator it) {
  // TODO: Lab 4.8 更新统计信息
  it->access_count += 1;
  if (it->access_count < k_) {
    cache_list_less_k.splice(cache_list_less_k.begin(), cache_list_less_k, it);
  } else if (it->access_count == k_) {
    cache_list_greater_k.splice(cache_list_greater_k.begin(), cache_list_less_k,
                                it);
  } else {
    cache_list_greater_k.splice(cache_list_greater_k.begin(),
                                cache_list_greater_k, it);
  }
}
} // namespace tiny_lsm
