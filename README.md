# Structure Database Engine Toy Project : bustub
## This repository is based on [Carnegi Melon University CMU 15-445/645](https://15445.courses.cs.cmu.edu/fall2021/assignments.html) [DATABASE SYSTEM course](https://youtu.be/v4bU6n97Vr8).
## 1. Project 1 : Buffer Pool Manager (clear at 3/7/2022)
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
* Comparing `BufferPoolManager::free_list_` to LRU list, free list is 'explicitly DELETED physical frame' list but LRU list store 'UNPINNED POTENTIALLY evicted physical frame' that is still hang on to physical frame
* Physical frame and Page table declare as `Page* page` and `std::unordered_map<page_id_t,frame_id_t> page_table`
### 3) Parallel Buffer Pool Manager
* By divide entire pool size by number of instance, rather than 1 instance manage whole pool
* Latch : Semaphore protecting SGA(Memory Resource, not block)
* Small critical section makes parallelism higher
   
## 2. Project 2 : Hash Table (clear at 18/7/2022)
### 1) Hash table bucket page, Hash table directory page
* Hash Table Directory manage 512 Hash table bucket page
* 512 local_depths_=512 bytes , 512 bucket_page_ids=2048 bytes
### 2) Extendible Hash Table
* Hashing 
   - To use MSB as Hash, `~0UL << (32 - global_depth_)` is used to Hash Mask
   - bucket frame is Logical Sequence of bucket index
   - substitute bucket frame to Physical Sequence(page id) by local depth and global depth
   - e.g. : ` Depth : 1`
   
   
![image](https://user-images.githubusercontent.com/81512075/182036389-a1030440-d2ae-4901-96e2-2f8b0f5d59dd.png)

   `  Depth :  2`

![image](https://user-images.githubusercontent.com/81512075/182036399-1485d6b6-32af-48f4-9731-6abc486a6068.png)

   `  Depth : 3`
   
![image](https://user-images.githubusercontent.com/81512075/182036406-a727026f-e691-48ff-8a38-f44bdf41cb7e.png)


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

## 3. Project 3 : Query Execution (progressing)
* Sequential Scan
* Insert

---------------------------------------------------
