#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

namespace tiny_lsm {

enum class IteratorType {
  SkipListIterator,
  MemTableIterator,
  SstIterator,
  HeapIterator,
  TwoMergeIterator,
  ConcactIterator,
  LevelIterator,
  Undefined,
};

class BaseIterator {
public:
  using value_type = std::pair<std::string, std::string>;
  using pointer = value_type *;
  using reference = value_type &;

  virtual BaseIterator &operator++() = 0;
  virtual bool operator==(const BaseIterator &other) const = 0;
  virtual bool operator!=(const BaseIterator &other) const = 0;
  virtual value_type operator*() const = 0;
  virtual IteratorType get_type() const = 0;
  virtual uint64_t get_tranc_id() const = 0;
  virtual bool is_end() const = 0;
  virtual bool is_valid() const = 0;
};

class SstIterator;
// *************************** SearchItem ***************************
struct SearchItem {
  std::string key_;
  std::string value_;
  uint64_t tranc_id_;
  int idx_;
  int level_; // 来自sst的level

  SearchItem() = default;
  SearchItem(std::string k, std::string v, int i, int l, uint64_t tranc_id)
      : key_(std::move(k)), value_(std::move(v)), idx_(i), level_(l),
        tranc_id_(tranc_id) {}
};

bool operator<(const SearchItem &a, const SearchItem &b);
bool operator>(const SearchItem &a, const SearchItem &b);
bool operator==(const SearchItem &a, const SearchItem &b);

// *************************** HeapIterator ***************************
class HeapIterator : public BaseIterator {
  friend class SstIterator;

public:
  HeapIterator() = default;
  HeapIterator(std::vector<SearchItem> item_vec, uint64_t max_tranc_id,
               bool filter_empty = true);
  pointer operator->() const;
  virtual value_type operator*() const override;
  BaseIterator &operator++() override;
  BaseIterator operator++(int) = delete;
  virtual bool operator==(const BaseIterator &other) const override;
  virtual bool operator!=(const BaseIterator &other) const override;

  virtual IteratorType get_type() const override;
  virtual uint64_t get_tranc_id() const override;
  virtual bool is_end() const override;
  virtual bool is_valid() const override;

private:
  bool top_value_legal() const;
  void skip_by_tranc_id();
  void update_current() const;
  bool advance_to_next();
  std::optional<SearchItem>
  select_visible_version(const std::vector<SearchItem> &candidates) const;

private:
  std::priority_queue<SearchItem, std::vector<SearchItem>,
                      std::greater<SearchItem>>
      items;
  std::optional<SearchItem> current_item_;
  mutable std::optional<value_type> cached_value_;
  uint64_t max_tranc_id_ = 0;
  bool filter_empty_ = true; // 是否过滤空值（删除标记）
};
} // namespace tiny_lsm
