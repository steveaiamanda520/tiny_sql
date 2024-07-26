#include "primer/trie.h"

#include <string_view>

#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  if (root_ == nullptr) {
    return nullptr;
  }

  auto node = root_;
  for (const auto &c : key) {
    auto iter = node->children_.find(c);
    if (iter == node->children_.end()) {
      return nullptr;
    }
    node = iter->second;
  }

  auto leaf = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  if (leaf == nullptr) {
    return nullptr;
  }

  return leaf->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  if (root_ == nullptr) {
    if (key.empty()) {
      return Trie(std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value))));
    }

    auto new_root = std::make_shared<TrieNode>();
    new_root->children_[key[0]] = Trie().Put(key.substr(1), std::move(value)).root_;
    return Trie(std::move(new_root));
  }

  if (key.empty()) {
    auto new_root = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    new_root->children_ = root_->children_;
    return Trie(std::move(new_root));
  }

  auto subtree = Trie(nullptr);
  if (auto iter = root_->children_.find(key[0]); iter != root_->children_.end()) {
    subtree = Trie(iter->second);
  }
  auto new_subtree = subtree.Put(key.substr(1), std::move(value));
  if (subtree.root_ == new_subtree.root_) {
    return *this;
  }

  auto new_root = root_->Clone();
  new_root->children_[key[0]] = new_subtree.root_;
  return Trie(std::move(new_root));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  if (root_ == nullptr) {
    return *this;
  }

  if (key.empty()) {
    if (!root_->is_value_node_) {
      return *this;
    }

    if (root_->children_.empty()) {
      return Trie{};
    }

    auto new_root = std::make_shared<TrieNode>();
    new_root->children_ = root_->children_;
    return Trie(std::move(new_root));
  }

  auto subtree = Trie(nullptr);
  if (auto iter = root_->children_.find(key[0]); iter != root_->children_.end()) {
    subtree = Trie(iter->second);
  }
  auto new_subtree = subtree.Remove(key.substr(1));
  if (subtree.root_ == new_subtree.root_) {
    return *this;
  }

  auto new_root = root_->Clone();
  if (new_subtree.root_ == nullptr) {
    new_root->children_.erase(key[0]);
    if (new_root->children_.empty() && !new_root->is_value_node_) {
      return Trie{};
    }

    return Trie(std::move(new_root));
  }

  new_root->children_[key[0]] = new_subtree.root_;
  return Trie(std::move(new_root));
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked
// up by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
