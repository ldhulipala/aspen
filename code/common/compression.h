#pragma once

// Implements byte-compression primitives

#define LAST_BIT_SET(b) (b & (0x80))
#define EDGE_SIZE_PER_BYTE 7

typedef unsigned char uchar;

using namespace std;

namespace compression {
  inline uintV read_first_neighbor(uchar const* &start, const uintV& src) {
    uchar fb = *start++;
    uintV edgeRead = (fb & 0x3f);
    if (LAST_BIT_SET(fb)) {
      int shiftAmount = 6;
      while (1) {
        uchar b = *start;
        edgeRead |= ((b & 0x7f) << shiftAmount);
        start++;
        if (LAST_BIT_SET(b))
          shiftAmount += EDGE_SIZE_PER_BYTE;
        else
          break;
      }
    }
    return (fb & 0x40) ? src - edgeRead : src + edgeRead;
  }

  inline uintV read_neighbor(uchar const* &start) {
    uintV edgeRead = 0;
    int shiftAmount = 0;

    while (1) {
      uchar b = *start++;
      edgeRead += ((b & 0x7f) << shiftAmount);
      if (LAST_BIT_SET(b))
        shiftAmount += EDGE_SIZE_PER_BYTE;
      else
        break;
    }
    return edgeRead;
  }

  /*
    Compresses the first edge, writing target-source and a sign bit.
  */
  long compress_first_neighbor(uchar *start, size_t offset, long source, long target) {
    long diff = target - source;
    long preCompress = diff;
    int bytesUsed = 0;
    uchar firstByte = 0;
    uintE toCompress = abs(preCompress);
    firstByte = toCompress & 0x3f; // 0011|1111
    if (preCompress < 0) {
      firstByte |= 0x40;
    }
    toCompress = toCompress >> 6;
    if (toCompress > 0) {
      firstByte |= 0x80;
    }
    start[offset] = firstByte;
    offset++;

    uchar curByte = toCompress & 0x7f;
    while ((curByte > 0) || (toCompress > 0)) {
      bytesUsed++;
      uchar toWrite = curByte;
      toCompress = toCompress >> 7;
      // Check to see if there's any bits left to represent
      curByte = toCompress & 0x7f;
      if (toCompress > 0) {
        toWrite |= 0x80;
      }
      start[offset] = toWrite;
      offset++;
    }
    return offset;
  }


//  inline size_t compress_first_neighbor(uchar* start, size_t offset,
//                                        uintV src, uintV ngh) {
//    intV value_and_sign = (intV) ngh - src;
//    intE value = abs(value_and_sign);
//    uchar cur_byte = value & 0x3f; // 0011|1111
//    if (value_and_sign < 0) {
//      cur_byte |= 0x40;
//    }
//    value = value >> 6;
//    if (value > 0) {
//      cur_byte |= 0x80;
//    }
//    start[offset++] = cur_byte;
//
//    cur_byte = value & 0x7f;
//    while ((cur_byte > 0) || (value > 0)) {
//      uchar to_write = cur_byte;
//      value = value >> 7;
//      // Check to see if there's any bits left to represent
//      cur_byte = value & 0x7f;
//      if (value > 0) {
//        to_write |= 0x80; // continue bit
//      }
//      start[offset++] = to_write;
//    }
//    return offset;
//  }

  inline size_t compress_neighbor(uchar* start, size_t cur_offset, uintV value) {
    uchar cur_byte = value & 0x7f;
    while ((cur_byte > 0) || (value > 0)) {
      uchar to_write = cur_byte;
      value = value >> 7;
      // Check to see if there's any bits left to represent
      cur_byte = value & 0x7f;
      if (value > 0) {
        to_write |= 0x80;
      }
      start[cur_offset] = to_write;
      cur_offset++;
    }
    return cur_offset;
  }

} // namespace compression
