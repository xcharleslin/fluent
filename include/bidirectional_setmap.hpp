//  Copyright 2018 U.C. Berkeley RISE Lab
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#ifndef FLUENT_INCLUDE_BIDIRECTIONAL_SETMAP_HPP_
#define FLUENT_INCLUDE_BIDIRECTIONAL_SETMAP_HPP_

#include <unordered_map>
#include <unordered_set>

/*
This bidirectional setmap implements a two-way set index
where looking up a row gives you a set of columns and vice versa.

Example:

bidirectional_setmap =

      A   B   C
    -------------
  1 | x | x |   |
    -------------
  2 |   | x |   |
    -------------
  3 |   |   |   |
    -------------

number_to_letters = bidirectional_setmap.row_to_cols_interface()
letter_to_numbers = bidirectional_setmap.col_to_rows_interface()

number_to_letters.get(1) -> {A, B}
letter_to_numbers.get(C) -> {}
letter_to_numbers.insert(C, 1)
number_to_letters.get(1) -> {A, B, C}
*/

// The unidirectional interface for actually using the setmap.
template<typename KeyT, typename ValueT>
class SetmapInterface {
 public:
  // Don't construct this class directly;
  // user constructs BidirectionalSetmap
  // and gets interfaces from that class.
  SetmapInterface() = delete;

  // See example above.
  // Key misses are fine and will return an empty set.
  const std::unordered_set<ValueT>& get(const KeyT& key) const {
    return forward[key];
  }

  // insert(key, value) postconditions:
  // this interface:  get(key) -> { ..., value, ...}
  // other interface: get(value) -> { ..., key, ...}
  void insert(const KeyT& key, const ValueT& value) {
    forward[key].insert(value);
    reverse[value].insert(key);
  }

  // Inverse of insert(key, value).
  void remove(const KeyT& key, const ValueT& value) {
    const auto& result_f = forward.find(key);
    const auto& result_r = reverse.find(value);
    if (result_f != forward.end() && result_r != reverse.end()) {
      forward[key].erase(value);
      reverse[value].erase(key);
    }
  }

  // Remove this key entirely from the index.
  void remove_key(const KeyT& key) {
    values = forward[key];
    for (const auto& value : values) {
      reverse[value].erase(key);
    }
    forward.erase(key);
  }

 protected:
  SetmapInterface(
      std::unordered_map<KeyT, std::unordered_set<ValueT>>& forward,
      std::unordered_map<ValueT, std::unordered_set<KeyT>>& reverse) {
    this->forward = forward;
    this->reverse = reverse;
  }

 private:
  std::unordered_map<KeyT, std::unordered_set<ValueT>>& forward;
  std::unordered_map<ValueT, std::unordered_set<KeyT>>& reverse;

};

// How to use:
// Make the BidirectionalSetmap with the types you want,
// and then get the two interfaces
// and use those interfaces to interact with it.
template<typename RowType, typename ColType>
class BidirectionalSetmap {
 public:
  BidirectionalSetmap() {
    row_to_cols_if = SetmapInterface(row_to_cols, col_to_rows);
    col_to_rows_if = SetmapInterface(col_to_rows, row_to_cols);
  }
  SetmapInterface<RowType, ColType>& row_to_cols_interface() {
    return row_to_cols_if;
  }
  SetmapInterface<ColType, RowType>& col_to_rows_interface() {
    return col_to_rows_if;
  }

 private:
  std::unordered_map<RowType, std::unordered_set<ColType>> row_to_cols;
  std::unordered_map<ColType, std::unordered_set<RowType>> col_to_rows;
  SetmapInterface<RowType, ColType> row_to_cols_if;
  SetmapInterface<ColType, RowType> col_to_rows_if;

};

#endif  // FLUENT_INCLUDE_BIDIRECTIONAL_SETMAP_HPP_
