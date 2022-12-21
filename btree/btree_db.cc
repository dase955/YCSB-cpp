//
//  btree_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 joechen <joechenrh@gmail.com>.
//

#include "btree_db.h"

#include "core/core_workload.h"
#include "core/db_factory.h"
#include "core/properties.h"
#include "core/utils.h"

#include <common/config.h>

#include <iostream>

namespace {
  const std::string PROP_NAME = "btree.dbname";
  const std::string PROP_NAME_DEFAULT = "";

  const std::string POOL_SIZE_NAME = "btree.pool_size";
  const std::string POOL_SIZE_NAME_DEFAULT = "134217728";

} // anonymous

namespace ycsbc {

cmudb::KVTable *BTreeDB::db_ = nullptr;
int BTreeDB::ref_cnt_ = 0;
std::mutex BTreeDB::mu_;

void BTreeDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;
  method_read_ = &BTreeDB::ReadSingle;
  method_scan_ = &BTreeDB::ScanSingle;
  method_update_ = &BTreeDB::UpdateSingle;
  method_insert_ = &BTreeDB::InsertSingle;
  method_delete_ = &BTreeDB::DeleteSingle;
  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));

  ref_cnt_++;
  if (db_) {
    return;
  }

  const std::string &db_path = props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
  if (db_path == "") {
    throw utils::Exception("BTree db path is missing");
  }

  int pool_size = std::stoi(props.GetProperty(POOL_SIZE_NAME, POOL_SIZE_NAME_DEFAULT));
  int page_num = pool_size / PAGE_SIZE;
  db_ = new cmudb::KVTable(db_path, page_num);
}

void BTreeDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);
  if (--ref_cnt_) {
    return;
  }
  delete db_;
}

void BTreeDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.name.data(), field.name.size());
    len = field.value.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.value.data(), field.value.size());
  }
}

void BTreeDB::DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                   const std::vector<std::string> &fields) {
  std::vector<std::string>::const_iterator filter_iter = fields.begin();
  while (p != lim && filter_iter != fields.end()) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    if (*filter_iter == field) {
      values.push_back({field, value});
      filter_iter++;
    }
  }
  assert(values.size() == fields.size());
}

void BTreeDB::DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                     const std::vector<std::string> &fields) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRowFilter(values, p, lim, fields);
}

void BTreeDB::DeserializeRow(std::vector<Field> &values, const char *p, const char *lim) {
  while (p != lim) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    values.push_back({field, value});
  }
}

void BTreeDB::DeserializeRow(std::vector<Field> &values, const std::string &data) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRow(values, p, lim);
}

DB::Status BTreeDB::ReadSingle(const std::string &table, const std::string &key,
                               const std::vector<std::string> *fields,
                               std::vector<Field> &result) {
  std::string data;
  if (!db_->Get(key, data)) {
    return kNotFound;
  }

  if (fields != nullptr) {
    DeserializeRowFilter(result, data, *fields);
  } else {
    DeserializeRow(result, data);
    assert(result.size() == static_cast<size_t>(fieldcount_));
  }
  return kOK;
}

DB::Status BTreeDB::ScanSingle(const std::string &table, const std::string &key, int len,
                               const std::vector<std::string> *fields,
                               std::vector<std::vector<Field>> &result) {
  auto it = db_->Seek(key);
  for (int i = 0; !it.isEnd() && i < len; i++) {
    auto v = (*it).second;
    std::string data;
    v.ToValue(data);
    ++it;

    result.push_back(std::vector<Field>());
    std::vector<Field> &values = result.back();
    if (fields != nullptr) {
      DeserializeRowFilter(values, data, *fields);
    } else {
      DeserializeRow(values, data);
      assert(values.size() == static_cast<size_t>(fieldcount_));
    }
  }
  return kOK;
}

DB::Status BTreeDB::UpdateSingle(const std::string &table, const std::string &key,
                                 std::vector<Field> &values) {
  return InsertSingle(table, key, values);
}

DB::Status BTreeDB::InsertSingle(const std::string &table, const std::string &key,
                                 std::vector<Field> &values) {
  std::string data;
  SerializeRow(values, data);
  db_->Put(key, data);
  return kOK;
}

DB::Status BTreeDB::DeleteSingle(const std::string &table, const std::string &key) {
  // Not used
  return kOK;
}

DB *NewBTreeDB() {
  return new BTreeDB;
}

const bool registered = DBFactory::RegisterDB("btreedb", NewBTreeDB);

} // ycsbc
