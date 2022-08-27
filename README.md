# Structure Database Engine Toy Project : bustub
## This repository is based on [Carnegi Melon University CMU 15-445/645](https://15445.courses.cs.cmu.edu/fall2021/assignments.html) [DATABASE SYSTEM course](https://youtu.be/v4bU6n97Vr8).
## 1. Project 1 : Buffer Pool Manager (clear at 3/7/2022)
![image](https://user-images.githubusercontent.com/81512075/185784815-97f64a67-004d-46f7-95e0-675e38d96acb.png)

### 1) LRU Replacer
![image](https://user-images.githubusercontent.com/81512075/185791548-b44eb3ae-7f50-4729-aa7c-926b4db0c486.png)

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
![image](https://user-images.githubusercontent.com/81512075/185791567-1bb14fd7-ea48-46e2-ad6a-a159a3515c5c.png)

* By divide entire pool size by number of instance, rather than 1 instance manage whole pool
* Latch : Semaphore protecting SGA(Memory Resource, not block)
* Small critical section makes parallelism higher
   
## 2. Project 2 : Hash Table (clear at 18/7/2022)
![image](https://user-images.githubusercontent.com/81512075/185791584-5fb842a8-f263-4fa4-9ae5-7325f3ff1e8a.png)

### 1) Hash table bucket page, Hash table directory page
![image](https://user-images.githubusercontent.com/81512075/185791595-ccfbaf95-3dca-441d-ab78-9e2db93a5bd1.png)
![image](https://user-images.githubusercontent.com/81512075/185791618-5f6984b3-a7ab-4bf0-9d61-85cad66b7d77.png)


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
      - Check CanShrink() to decrease global depth, which is recursive function
### 3) Concurrency
* std::lock_guard lock(latch_)

## 3. Project 3 : Query Execution (clear at 8/8/2022)
* Overall Structure
* Predicate
* Sequential Scan
   - implementation defect : No considiration about sub query ( e.g. SELECT * from (SELECT * FROM table1) )
* Insert
   - Raw Insert, SubQuery Insert
* Delete
* Indexing
   - Insert, Scan, Update, Delete
* Update
* Naive(stupid) block join
* Hash Join
   - `std::unordered_map<hash_t,std::pair<RID,Tuple>>` : unordered map is implemented with Hash Table
   - implementation defect : If there are OVERLAPPED join keys(value) in left table , it would malfunctioned . 
* Aggregation
   - sum, count, min, max
   - Group By , having
* Limit
* Distinct
   - implementation defect : There is no column index of Distinct Column (cannot deal with ` SELECT DISTINCT colC, sum(colA), count(colB) FROM test_7`)

## 4. Project 4 : Concurrency (progressing)
* LockManager
   - 2PL
   - DeadLock Prevention (based on timestamp)
      - Wound Wait, If Requesting txn have high priority(low timestamp) than Lock-Holding txn, the low priority(high timestamp)
         releases lock and aborts.
   - If Isolation Level == ReadUncommited, Shared Lock cannot be allowed
* TransactionManager
<img src = "https://user-images.githubusercontent.com/81512075/187020344-6bb00ffa-cd02-4fb4-a1be-e58769d070b7.png" width="500" height="300">\

![image](https://user-images.githubusercontent.com/81512075/187020350-799981e5-3018-4264-bcea-19d96ca87601.png)


## 5. Recovery
---------------------------------------------------
