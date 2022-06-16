/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

#include "bufHashTbl.h"
#include "file_iterator.h"

namespace badgerdb
{

  constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

  //----------------------------------------
  // Constructor of the class BufMgr
  //----------------------------------------

  BufMgr::BufMgr(std::uint32_t bufs)
      : numBufs(bufs),
        hashTable(HASHTABLE_SZ(bufs)),
        bufDescTable(bufs),
        bufPool(bufs)
  {
    for (FrameId i = 0; i < bufs; i++)
    {
      bufDescTable[i].frameNo = i;
      bufDescTable[i].valid = false;
    }

    clockHand = bufs - 1;
  }

  void BufMgr::advanceClock()
  {
    if (clockHand + 1 >= numBufs)
    {
      clockHand = 0;
    }
    else
    {
      clockHand++;
    }
  }

  void BufMgr::allocBuf(FrameId &frame)
  {
    advanceClock();
    bool allocated = false;
    std::vector<bool> pinned(numBufs, false);
    uint32_t numPinned = 0;
    while (!allocated)
    {
      if (numPinned >= numBufs)
      {
        throw BufferExceededException();
      }
      if (bufDescTable[clockHand].valid) {
        if (bufDescTable[clockHand].refbit)
        {
          bufDescTable[clockHand].refbit = false;
          advanceClock();
          continue;
        }
        else if (bufDescTable[clockHand].pinCnt > 0)
        {
          if (!pinned[clockHand]) {
            pinned[clockHand] = true;
            numPinned++;
          }
          advanceClock();
          continue;
        }
        else if (bufDescTable[clockHand].dirty)
        {
          bufDescTable[clockHand].file.writePage(bufPool[clockHand]);
        }
        hashTable.remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
      } 
        frame = bufDescTable[clockHand].frameNo;
        allocated = true;
    }
  }

  
  void BufMgr::readPage(File &file, const PageId pageNo, Page *&page)
  {
    // check if the page is already in the buffer pool via lookup method
    FrameId f;

    try
    {
      hashTable.lookup(file, pageNo, f);
      // page is in the buffer pool:
      bufDescTable[f].refbit = true;
      bufDescTable[f].pinCnt += 1;
      page = &bufPool[f];
    }
    catch (const HashNotFoundException &e)
    {
      // page is not in the buffer pool:
      Page p = file.readPage(pageNo);
      allocBuf(f);
      bufPool[f] = p;
      hashTable.insert(file, pageNo, f);
      bufDescTable[f].Set(file, pageNo);
      page = &bufPool[f];
    }
  }

  void BufMgr::unPinPage(File &file, const PageId pageNo, const bool dirty)
  {
    FrameId fid;
    try
    {
      hashTable.lookup(file, pageNo, fid);
    }
    catch (const HashNotFoundException &e)
    {
      std::cerr << e.message();
      return;
    }
    if (bufDescTable[fid].pinCnt > 0)
    {
      bufDescTable[fid].pinCnt -= 1;
      if (dirty)
      {
        bufDescTable[fid].dirty = true;
      }
    }
    else
    {
      throw PageNotPinnedException(file.filename(), pageNo, fid);
    }
  }

  void BufMgr::allocPage(File &file, PageId &pageNo, Page* &page)
  {
    FrameId fid;
    allocBuf(fid);
    bufPool[fid] = file.allocatePage();
    page = &bufPool[fid];
    pageNo = bufPool[fid].page_number();
    hashTable.insert(file, pageNo, fid);
    bufDescTable[fid].Set(file, pageNo);
  }

  void BufMgr::flushFile(File &file)
  {
    for (FrameId i = 0; i < numBufs; i++) {
      if (bufDescTable[i].file==file) {
        if (!bufDescTable[i].valid) {
          throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
        }
        if (bufDescTable[i].pinCnt > 0) {
          throw PagePinnedException(file.filename(), bufDescTable[i].pageNo, i);
        }
        if (bufDescTable[i].dirty) {
          file.writePage(bufPool[i]);
          bufDescTable[i].dirty = false;
        }
        hashTable.remove(file, bufDescTable[i].pageNo);
        bufDescTable[i].clear();
      }
    }
  }

  void BufMgr::disposePage(File &file, const PageId PageNo)
  {
    
    FrameId fid;
    bool frameAllocated = true;
    try
    {
      hashTable.lookup(file, PageNo, fid);
    }
    catch (const HashNotFoundException &e)
    {
      frameAllocated = false;
    }
    if (frameAllocated)
    {
      bufDescTable[fid].clear();
      hashTable.remove(file, PageNo);
    }
    file.deletePage(PageNo);
  }

  void BufMgr::printSelf(void)
  {
    int validFrames = 0;

    for (FrameId i = 0; i < numBufs; i++)
    {
      std::cout << "FrameNo:" << i << " ";
      bufDescTable[i].Print();

      if (bufDescTable[i].valid)
        validFrames++;
    }

    std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
  }

} // namespace badgerdb
