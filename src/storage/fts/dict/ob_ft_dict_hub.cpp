/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ft_dict_hub.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/dict/ob_ft_cache_container.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_range_dict.h"
namespace oceanbase
{
namespace storage
{
int ObFTDictHub::init()
{
  static constexpr int K_MAX_DICT_BUCKET = 128; // for now, only built-in dicts.
  int ret = OB_SUCCESS;
  if (OB_FAIL(dict_map_.create(K_MAX_DICT_BUCKET, "dict_map"))) {
  } else if (OB_FAIL(rw_dict_lock_.init(K_MAX_DICT_BUCKET))) {
  } else {
    is_inited_ = true;
  }
  return ret;
};

int ObFTDictHub::destroy()
{
  int ret = OB_SUCCESS;
  {
    ObLockGuard<common::ObSpinLock> guard(shared_lock_);
    for (int64_t i = 0; i < SHARED_DICT_CNT; ++i) {
      shared_dicts_[i] = nullptr;
      shared_ranges_[i] = nullptr;
    }
    shared_alloc_.reset();
  }
  is_inited_ = false;
  return ret;
}
int ObFTDictHub::build_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  ObFTDictInfo info;
  container.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObBucketHashWLockGuard guard(rw_dict_lock_, key.hash());

    // try if valid with no recursive lock
    if (OB_FAIL(get_dict_info(key, info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // dict not exist, make new one, by caller
        ret = OB_ENTRY_NOT_EXIST;
      } else {
      }
    } else if (OB_FAIL(ObFTRangeDict::try_load_cache(desc, info.range_count_, container))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
      } else {
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(ObFTRangeDict::build_cache_from_ik_dict(desc, container))) {
        } else if (FALSE_IT(info.range_count_ = container.get_handles().size())) {
        } else if (OB_FAIL(put_dict_info(key, info))) {
        }
      }
    }
  }
  return ret;
}

int ObFTDictHub::load_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  ObFTDictInfo info;
  container.reset();
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    {
      ObBucketHashRLockGuard guard(rw_dict_lock_, key.hash());
      if (OB_FAIL(get_dict_info(key, info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // dict not exist, make new one, by caller
          ret = OB_ENTRY_NOT_EXIST;
        } else {
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObFTRangeDict::try_load_cache(desc, info.range_count_, container))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // dict not exist, make new one, by caller
      } else {
      }
    }
  }

  return ret;
}


int ObFTDictHub::shared_dict_slot(const ObFTDictDesc &desc, int64_t &slot) const
{
  int ret = OB_SUCCESS;
  switch (desc.type_) {
  case ObFTDictType::DICT_IK_MAIN: {
    slot = 0;
  } break;
  case ObFTDictType::DICT_IK_QUAN: {
    slot = 1;
  } break;
  default: {
    slot = -1;
    ret = OB_NOT_SUPPORTED;
  } break;
  }
  return ret;
}

int ObFTDictHub::get_shared_dict(const ObFTDictDesc &desc, const ObIFTDict *&dict)
{
  int ret = OB_SUCCESS;
  int64_t slot = -1;
  dict = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(shared_dict_slot(desc, slot))) {
  } else {
    ObLockGuard<common::ObSpinLock> guard(shared_lock_);
    if (OB_NOT_NULL(shared_dicts_[slot])) {
      dict = shared_dicts_[slot];
    } else {
      ObFTCacheRangeContainer *container = nullptr;
      ObFTRangeDict *range_dict = nullptr;
      if (OB_ISNULL(container = OB_NEWx(ObFTCacheRangeContainer, &shared_alloc_, shared_alloc_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_FTS_LOG(WARN, "Failed to allocate dict range container", K(ret));
      } else if (OB_FAIL(load_cache(desc, *container))) {
        // The range blocks are not cached yet; build them once and keep them.
        if (OB_ENTRY_NOT_EXIST != ret) {
          STORAGE_FTS_LOG(WARN, "Failed to load dict cache", K(ret));
        } else if (OB_FAIL(build_cache(desc, *container))) {
          STORAGE_FTS_LOG(WARN, "Failed to build dict cache", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(container->get_handles().empty())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_FTS_LOG(WARN, "Empty dict cache container", K(ret));
      } else if (OB_ISNULL(range_dict = OB_NEWx(ObFTRangeDict, &shared_alloc_, shared_alloc_,
                                                container, desc))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_FTS_LOG(WARN, "Failed to allocate shared range dict", K(ret));
      } else if (OB_FAIL(range_dict->init())) {
        STORAGE_FTS_LOG(WARN, "Failed to init shared range dict", K(ret));
      } else {
        shared_ranges_[slot] = container;
        shared_dicts_[slot] = range_dict;
        dict = range_dict;
      }
    }
  }
  return ret;
}

int ObFTDictHub::get_dict_info(const ObFTDictInfoKey &key, ObFTDictInfo &info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dict_map_.get_refactored(key, info))) {
    if (OB_HASH_NOT_EXIST != ret) {
    }
  }

  return ret;
}

int ObFTDictHub::put_dict_info(const ObFTDictInfoKey &key, const ObFTDictInfo &info)
{
  int ret = OB_SUCCESS;
  const int cover_exist_flag = 1;
  if (OB_FAIL(dict_map_.set_refactored(key, info, cover_exist_flag))) {
  }

  return ret;
}
} //  namespace storage
} //  namespace oceanbase
