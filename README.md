# Structure Database Engine Toy Project : bustub

## 1. Project 1 (clear at 3/7/2022)
### 1) LRU Replacer
* Last Recently Used Algorithm by double linked list
* Unpinned Pages' physical frame id is hang on list
* Most recently used unpinned page is hang on front of list.
* Implementation)
   - If Pin, frame_id is delete from list. O(N)
   - If Unpin, frame id is add to list. O(N)
   - If Victim, At the end of list is removed and return its value. O(1)
### 2) Buffer Pool Manager
* Managing class Page in physical frame, which is VM Concept
* Comparing BufferPoolManager::free_list_ to LRU list, free list is 'explicitly DELETED physical frame' list but LRU list store 'UNPINNED POTENTIALLY evicted physical frame' that is still hang on to physical frame
* Physical frame and Page table declare as Page* page and std::unordered_map<page_id_t,frame_id_t> page_table
### 3) Parallel Buffer Pool Manager
* By divide entire pool size by number of instance, rather than 1 instance manage whole pool
* Latch : Semaphore protecting SGA(Memory Resource, not block)
* Small critical section makes parallelism higher
   
## 2. Project 2 (clear at 18/7/2022)
### 1) Hash table bucket page, Hash table directory page
* Hash Table Directory manage 512 Hash table bucket page
* 512 local_depths_=512 bytes , 512 bucket_page_ids=2048 bytes
### 2) Extendible Hash Table
* Hashing 
   - To use MSB as Hash, `~0UL << (32 - global_depth_)` is used to Hash Mask
   - bucket frame is Logical Sequence of bucket index
   - substitute bucket frame to Physical Sequence(page id) by local depth and global depth
   - e.g. : 
   `   Depth : 1`
   `   Frame	0	1`
   `   Index	0	256`
   `   PageId	1	2	`

   `  Depth :  2`
   `  Frame    00	01	10	11`
   `  Index	   0  128	256	384`
   `  PageId	1	3	2	2 `
* Insert
   - If Bucket is already Full
      - If local depth is equal to global depth, need to set ALL buckets local depth to point accurate bucket
      - Rehashing all components of the bucket and to copy those to new bucket
      - reexecute Insert (recursive call)
* GetValue
   - Hash Key to find correct bucket index (Function GetBucketIdxByKey)
   - retrive page id by bucket index, and execute HashTableBucketPage->GetValue
* Remove
   - If Bucket is Empty after Remove
      - If Empty Bucket remains after merged to Buddy Bucket, Copy All Contents of Buddy Bucket to Empty Bucket  : `Empty  ||interval|| Buddy`
      - If Buddy Bucket remains after merged to Empty Bucket : ` Buddy ||interval|| Empty`
      - Decrease local depth of Buddy/Empty Buckets
      - Check CanShrink() to decrease global depth
### 3) Concurrency
* Use Two Phase Lock(Latch) to prevent Deadlock



---------------------------------------------------
