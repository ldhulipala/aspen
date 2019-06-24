#pragma once

#include <limits>

namespace ops {

  static constexpr uint64_t MAX_THREADS = 144;
  static constexpr uint64_t ZERO = 0;
  static constexpr uint64_t ONE = 1;
  static constexpr uint64_t MASK_FLAG = (ONE << 2)-ONE;
  static constexpr uint64_t MASK_INDEX = ((ONE << 10)-ONE) << 2;
  static constexpr uint64_t MASK_TIMESTAMP = (~ZERO) - MASK_INDEX - MASK_FLAG;
  static constexpr uint64_t MASK_VERSION = (~ZERO) - MASK_FLAG;
  static constexpr uint64_t EMPTY = ((uint64_t(MAX_THREADS)+10) << 2);

  inline uint64_t get_idx(uint64_t x)
  {
      return (x&MASK_INDEX) >> 2;
  }

  inline uint64_t get_timestamp(uint64_t x)
  {
      return (x&MASK_TIMESTAMP) >> 12;
  }

  inline uint64_t get_version(uint64_t x)
  {
      return (x&MASK_VERSION);
  }

  inline uint64_t get_flag(uint64_t x)
  {
      return (x&MASK_FLAG);
  }

  // timestamp is a 52 bit int
  // timestamp index is a 10 bit int?
  // flag is a bit (use 2 bits for some reason)
  inline uint64_t combine(uint64_t index, uint64_t timestamp, uint64_t flag)
  {
      return (timestamp << 12) + (index << 2) + flag;
  }

  inline uint64_t combine(uint64_t version, uint64_t flag)
  {
      return version + flag;
  }
};


