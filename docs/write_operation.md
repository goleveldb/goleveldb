# LevelDB 高性能写操作

了解`LevelDB`对外暴露的接口使用方法，并揭示其数据结构、执行流程。





# 1-将数据写入**LevelDB**

## 1.1-通过WriteBatch批量操作

```C++
int main () {
    // 1. 打开leveldb.
    auto db = GetLevelDB();

    // 2. 创建写任务并且进行操作.
    leveldb::WriteBatch batch;
    batch.Put("key1", "value1");
    batch.Put("key2", "value2");
    batch.Put("key1". "value1-1");

    batch.Delete("key2");

    // 3. 执行写任务.
    auto write_status = db->Write(leveldb::WriteOptions(), &batch);
    if (!write_status.ok()) return EXIT_FAILURE;
}
```

用户可以通过`db->Write`方法，对数据库进行插入、修改、删除操作。`db->Write`方法需要两个参数：

1. `WriteOptions`：通过`sync`字段，设置本次操作是否直接落盘；
2. `WriteBatch`: 本次进行的所有操作；

## 1.2-通过Put/Delete直接操作

```C++
// 直接Set.
db->Put(leveldb::WriteOptions(), "key1", "value1-2");
// 直接Delete.
db->Delete(leveldb::WriteOptions(), "key2");
```

有时，我们并不想对数据库进行批量操作，也可以通过`db->Put`和`db->Delete`直接修改数据库。但是对于单次操作而言，`Put`与`Delete`方法的执行效率与构造`batch`、执行`Write`方法的效率完全一致，其实现如下：

```C++
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
    WriteBatch batch;
    batch.Put(key, value);
    return Write(opt, &batch);
}
```

可以从上面的代码中看出，`Put`方法就是将新建`WriteBatch`、`db->Write(batch)`的过程进行了封装。那么这个`WriteBatch`对外暴露了哪些接口，它内部的数据又是如何组织、存储的呢？



# 2-WriteBatch的功能与存储结构

## 2.1-WriteBatch的定义

```C++
class WriteBatch {
 public:
  class Handler {
   public:
    virtual ~Handler();
    virtual void Put(const Slice& key, const Slice& value) = 0;
    virtual void Delete(const Slice& key) = 0;
  };

  ~WriteBatch();

  // Store the mapping "key->value" in the database.
  void Put(const Slice& key, const Slice& value);

  // If the database contains a mapping for "key", erase it.  Else do nothing.
  void Delete(const Slice& key);

  // Clear all updates buffered in this batch.
  void Clear();

  // The size of the database changes caused by this batch.
  size_t ApproximateSize() const;

  // Copies the operations in "source" to this batch.
  void Append(const WriteBatch& source);

  // Support for iterating over the contents of a batch.
  Status Iterate(Handler* handler) const;

 private:
  friend class WriteBatchInternal;
  std::string rep_;  // See comment in write_batch.cc for the format of rep_
};
```

`WriteBatch`记录用户对数据库进行的操作，用户可以通过`batch.Put`以及`batch.Delete`向`WriteBatch`中追加变更，也可以通过`batch.Clear`将`batch`中的操作清空。如果存在多个`batch`，用户也可以通过`batch.Append`将其他`batch`加入当前的`batch`中。

同时，为了操作`WriteBatch`，`LevelDB`提供了`Batch::Handler`类，用户可以实现`Batch::Hanlder`，并且调用`batch->Iterate`方法，使用`Handler`依次对`Batch`中的数据进行操作。

现在我们已经对`WriteBatch`功能有了基本的了解，通过观察其成员可以得知，所有的数据都是存储在`rep_`变量中的，那么这些`Put`, `Delete`数据是如何存储的呢？

## 2.2-WriteBatch中的存储结构

<center>
    <img src="https://goleveldb-1301596189.cos.ap-guangzhou.myqcloud.com/LevelDB%E8%AE%BE%E8%AE%A1%E4%B8%8E%E5%AE%9E%E7%8E%B0-WriteBatch%E6%80%BB%E4%BD%93%E6%A0%BC%E5%BC%8F.drawio.svg"/>
    <p>
        <b>图1：WriteBatch存储结构</b>
    </p>
</center>


`WriteBatch`结构如上图所示，每个`WriteBatch`都由`12`字节的定长头部与后面的`datas`部分组成。头部中的`sequence`记录着`WriteBatch`首个数据的序列号，`count`记录了`datas`中操作的数量。`datas`类似一个数组，其中的每个元素对应用户录入的一个操作。



<center>
    <img src="https://goleveldb-1301596189.cos.ap-guangzhou.myqcloud.com/LevelDB%E8%AE%BE%E8%AE%A1%E4%B8%8E%E5%AE%9E%E7%8E%B0-WriteBatch%E4%B8%AD%E7%9A%84data.drawio.svg"/>
    <p>
        <b>图2：WriteBatch中的Data</b>
    </p>
</center>


`datas`中的元素如上图所示，`data`的首个字节`tag`标识当前数据的操作类型，`kTypeValue`表示修改操作，而`kTypeDeletion`表示删除操作。当操作为`Delete`时候，后续部分记录需要删除的`Key`，而操作为`Put`时，后续部分会记录`Key`与`Value`。对于`<Key, Value>`，会先使用`VarInt`编码其长度，再直接记录其`Slice`值。

那么用户在调用`Put`, `Delete`, `Clear`, `Append`时，`WriteBatch`实际上是在根据上述规则维护其`rep_`成员变量。



# 3-Write批量写操作执行过程

从前两节中，我们了解到`LevelDB`对数据的修改实际上都是通过`db->Write`方法，提交`WriteBatch`来进行的。在本节中我们会进入到`DBImpl::Write`方法中，分析`Write`操作的实际运行过程。

## 3.1-Write操作流程概要

`Write`操作的目标是将`WriteBatch`先写入`Log`，再将`WriteBatch`的操作同步到`MemTable`中。考虑最简单场景下的串行化实现，只需要依次对`Log`、`MemTable`进行相应操作即可。

但由于实际使用场景对性能有所要求，并且用户可以会使用多个线程并行的调用`Write`方法，因此在该过程中`LevelDB`做了下述优化：

1. 并发控制：多个线程共同调用`Write`时会被强制串行化，来保证线程安全；
2. `WriteBatch`合并：多个等待执行的`WriteBatch`在特定条件下可以合并。合并操作可以降低落盘次数，这一策略对开启`sync`的写操作提升更为明显；



那么具体来说，`LevelDB`中的一次`Write`操作流程如下图所示：

<center>
    <img src="https://goleveldb-1301596189.cos.ap-guangzhou.myqcloud.com/LevelDB%E8%AE%BE%E8%AE%A1%E4%B8%8E%E5%AE%9E%E7%8E%B0-Write%E6%89%A7%E8%A1%8C%E6%B5%81%E7%A8%8B.drawio.svg"/>
    <p>
        <b>图3：Write操作流程</b>
    </p>
</center>




那么在接下来的篇幅中，我们就会详细介绍`Write`操作各个过程。

## 3.2-Write操作的并发控制

### 3.2.1-并发控制原理

<center>
    <img src="https://goleveldb-1301596189.cos.ap-guangzhou.myqcloud.com/LevelDB%E8%AE%BE%E8%AE%A1%E4%B8%8E%E5%AE%9E%E7%8E%B0-Write%E6%93%8D%E4%BD%9C%E5%B9%B6%E5%8F%91%E6%8E%A7%E5%88%B6.drawio.svg"/>
    <p>
        <b>图4：Write操作串行化</b>
    </p>
</center>


如上图所示，用户调用`db->Write`时会将用户的调用记录到`Writer`结构中（这里的`Writer`可以被视为一个`Task`），这些`Writer`会被加入到一个等待队列中，当前线程的`Writer`成为等待队列的头部时，会被唤醒，并且执行。

这样的设计就保证了写任务能够按照用户的调用顺序执行，而且在同一时间只有一个线程在执行写任务。

### 3.2.2-并发控制实现

接下来进入`Write`操作的源码，来分析`Writer`具体是如何串行执行的。

**1. 创建Writer并等待**

```C++
// 创建writer
Writer w(&mutex_);
w.batch = updates;
w.sync = options.sync;
w.done = false;

// 通过一个全局的mutex创建了一个类似于lock_guard的RAII锁.
MutexLock l(&mutex_);
writers_.push_back(&w);

// 1. 如果当前的任务不在队列头部，那么就需要等待.
// 2. 如果当前任务没有被完成，那么也需要等待。
// w.done刚刚被赋值为false, 为什么还要判断呢？这个问题我们一会就会讲到.
while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
}

// 别的线程已经把这个writer执行完了，那么就直接返回即可.
// 在什么情况下当前线程加入的writer会被其它线程执行呢？需要接着来看.
if (w.done) {
    return w.status;
}
```

在执行`Write`函数时，会创建一个`Writer`，这个`Writer`相当于一个写任务，它记录了`Write`操作的信息以及执行结果。接下来，将这个`Writer`加入到`writers_`队列中，当这个`Writer`不是队列头时，就会调用`w.cv.Wait()`方法阻塞住当前的线程，等待其他任务执行结束后，将当前线程重新唤醒、执行。

那么，`w.done`这一判断到底覆盖了哪种情况呢？我们后面再讲。



**2. 操作结束后的唤醒**

```C++
// last_writer 最后一个被执行的Writer.

// 已执行完毕的writer，记录结果并且唤醒.
while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
        ready->status = status;
        ready->done = true;
        ready->cv.Signal();
    }
    if (ready == last_writer) break;
}

// 唤醒下一个待执行的writer线程.
if (!writers_.empty()) {
    writers_.front()->cv.Signal();
}
```

一个线程会一次性顺序合并并执行多个`Writer`，被执行的最后一个`Writer`被标记为`last_writer`。那么从`writers_.front()`到`last_writer`之间的这些`writer`都需要被标记为已执行完毕的状态，并且调用`Singnal`方法来唤醒正在等待的线程。该线程被唤醒后，会检测到`w.don == true`，因而跳出等待的`while`循环，并且在下面的`if`中因为任务已经结束而退出。

接下来，如果等待队列中还有待执行的`Writer`，也会将第一个待执行的`Writer`唤醒。

### 3.2.2-并发控制总结

总体来说，`Write`操作在执行过程中，会将任务存储到`Writer`结构中，然后加入到`writers_`队列中等待执行。在同一时刻只有一个线程在实际执行`Write`任务，执行`Write`任务的线程一次会从`writers_`队列中取出多个`Writer`进行合并执行。而对于所属任务被合并执行的线程而言，在唤起时发现`w.done == true`，任务已经被执行，因此直接退出即可。

上述机制保证了`LevelDB`的写操作能够保证线程安全，并且能够按照用户调用顺序执行。但是，`LevelDB`是如何保证内存表可写、`Writer`又是如何被合并的呢？

接下来的篇幅中，我们会了解当一个`write`线程被唤醒后是如何执行`Writer`的。



## 3.3-MakeRoomForWrite的功能和实现

```C++
if (w.done) {
    return w.status;
}

// May temporarily unlock and wait.
Status status = MakeRoomForWrite(updates == nullptr);
```

如上述代码所示，当进程被唤醒后，发现任务没有被执行时，他会开始处理当前的`Writer`，在处理`Writer`前，需要确保当前的内存表`MemTable`可写，而这一操作就是通过`MakeRoomForWrite`函数实现的。

```C++
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
      // 判断mem_table是否可写.
      // - 可写时直接退出循环，完成等待.
      // - 不可写时根据原因进行处理、进行等待.
  }
  return s;
}
```

`MakeRoomForWrite`函数如上面的代码所示，整体来说是一个大循环，当前条件下可以写`Memtable`时就会退出循环，当前条件下不可写`Memtable`时就会根据不可写的原因进行处理，并且继续循环。接下来需要重点了解`while`循环内的部分，知道什么情况下不可写、如何处理。

### 3.3.1-后台线程出错，直接退出

```C++
if (!bg_error_.ok()) {
    // Yield previous error
    s = bg_error_;
    break;
}
```

这个就不多讲了。

### 3.3.2-Level0文件较多，延迟写

```C++
else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {
    // 其中，kL0_SlowdownWritesTrigger = 8
    // We are getting close to hitting a hard limit on the number of
    // L0 files.  Rather than delaying a single write by several
    // seconds when we hit the hard limit, start delaying each
    // individual write by 1ms to reduce latency variance.  Also,
    // this delay hands over some CPU to the compaction thread in
    // case it is sharing the same core as the writer.
    mutex_.Unlock();
    env_->SleepForMicroseconds(1000);
    allow_delay = false;  // Do not delay a single write more than once
    mutex_.Lock();
}
```

`LevelDB`中，数据被存储在很多个`Level`中，内存表被写满后，数据会被下放到`Level0`中，而当`Level0`中的文件过多时，会有后台线程将`Level0`中的数据合并到`Level1`中，以此类推。此时发现`Level0`中文件过多，那么就说明需要后台线程对文件进行合并，因此需要放慢数据新增速度，来避免`MemTable`增长过快，继续往`Level0`中写数据。

简单来说就是：太多啦！写慢点吧！

### 3.3.3-MemTable可写，直接退出

```C++
else if (!force &&
         (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
    // There is room in current memtable
    break;
}
```

这个也很简单，就是`mem_`的已用空间小于等于设置中的可接受内存空间，那么就认为可以继续写内存。

### 3.3.4-Memtable正在被压缩，等待

```C++
if (imm_ != nullptr) {
    // We have filled up the current memtable, but the previous
    // one is still being compacted, so we wait.
    Log(options_.info_log, "Current memtable full; waiting...\n");
    background_work_finished_signal_.Wait();
}
```

`LevelDB`中的内存表分为`mem_`和`imm_`，当内存表写满后，会被压缩为`imm_`并且在后台开启一个线程写入到`Level0`中，这里的情况表示`mem_`刚刚被写满，正在被压缩为`imm_`，因此需要等待。

### 3.3.5-Level0文件太多，等待

```C++
else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
    // kL0_StopWritesTrigger = 12
    // There are too many level-0 files.
    Log(options_.info_log, "Too many L0 files; waiting...\n");
    background_work_finished_signal_.Wait();
}
```

这里的情况与`3.3.2`的情况是类似的，但是这里`Level0`的文件更多，已经无法接受进一步的增长了，因此直接停掉，等待`Level0`被合并到下层再继续写。

### 3.3.5-Memtable无空间，IMM为空，将Memtable复制给IMM

```C++
else {
    // Attempt to switch to a new memtable and trigger compaction of old
    assert(versions_->PrevLogNumber() == 0);
    uint64_t new_log_number = versions_->NewFileNumber();
    WritableFile* lfile = nullptr;
    s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
    if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
    }
    delete log_;
    delete logfile_;
    logfile_ = lfile;
    logfile_number_ = new_log_number;
    log_ = new log::Writer(lfile);
    imm_ = mem_;
    has_imm_.store(true, std::memory_order_release);
    mem_ = new MemTable(internal_comparator_);
    mem_->Ref();
    force = false;  // Do not force another compaction if have room
    MaybeScheduleCompaction();
}
```

这种情况表明，当前的`Memtable`已经被写满，我们需要将`Memtable`中的数据转移到`imm`中，再开启后台线程将`imm_`中的数据合并入`Level0`。

这部分的操作我们先不详细讲，后面的`Compaction`操作的讲解中，会有详细的讲解。

### 3.3.6-总结

上面的就是所有的情况了，总结一下就是`MakeRoomForWrite`实际上就是在等待`Memtable`可写，所有不可写的情况都会进行相应的处理，然后再`continue`回到循环。只有当`Memtable`空间充足时才会退出循环继续执行。



## 3.4-合并Writer并执行

最后进入`Write`操作的核心部分：

```C++
// May temporarily unlock and wait.
Status status = MakeRoomForWrite(updates == nullptr);
uint64_t last_sequence = versions_->LastSequence();
Writer* last_writer = &w;
if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    // 合并Writer，合并到一个新的write_batch中.
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
        // 将合并后的Writer写入日志&内存表.
        mutex_.Unlock();
        status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
        bool sync_error = false;
        if (status.ok() && options.sync) {
            status = logfile_->Sync();
            if (!status.ok()) {
                sync_error = true;
            }
        }
        if (status.ok()) {
            status = WriteBatchInternal::InsertInto(write_batch, mem_);
        }
        mutex_.Lock();
        if (sync_error) {
            // The state of the log file is indeterminate: the log record we
            // just added may or may not show up when the DB is re-opened.
            // So we force the DB into a mode where all future writes fail.
            RecordBackgroundError(status);
        }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
}
```

在`MakeRoomForWrite`后，已经确定可以写`Memtable`，接下来就需要对`Writer`进行合并操作，将多个可以一起执行的`Writer`合并到一起在进行执行。所谓执行就是通过`log_->AddRecord`将其加入日志中，然后再通过`WriteBatchInternal::InsertInto`将其提交到内存表中。这一过程中最为核心的操作就是`BuildBatchGroup`即合并操作。

在讨论如何合并前，我们先来讨论为什么要合并。在完成这样一个单机存储引擎的时候，我们往往关心它的吞吐量和操作延迟，即：

- 每秒能够读/写多少数据
- 读/写一次数据需要多少实现

对于一个持久化数据库而言，最耗时的操作就是落盘，即将内存中的数据写入磁盘。为了提升落盘速度，我们会将数据先写入缓冲区，再一并落盘。但是在某些场景下，用户为了保证数据的安全，愿意牺牲一部分的性能，即指定`sync`。用户指定`sync`时，就希望每次执行完毕后，就将日志落盘。

那我们假设，用户疯狂并发使用`Put`接口，提交了很多`kv`，每个`kv`都不大，但是需要频繁落盘。这样就会导致吞吐量变低，这是我们不希望看到的。因此，就有了合并`Writer`的机制，我们在处理前，就对一些操作进行合并，合并后一同执行，这样执行的结果与用户的期待相同，也能够提升吞吐量。

那么接下来，我们就来看，这个合并的操作到底是如何实现的。

### 3.4.1-BuildBatchGroup合并Writer的实现

**1. 初始化、设置参数**

```C++
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }
```

这里会根据当前`Writer`的`Batch`大小来设置合并后`WriteBatch`的最大`Size`。我们刚才讲过吞吐量与延迟的概念，用户希望提高吞吐量，但是希望延迟不受影响，那么如果当前的`Writer`是一个小`Writer`，那么用户也希望这次写操作的延迟更低，因此再`siz <= (128 << 10)` 时，`max_size`较小。



**2. 合并**

```C++
*last_writer = first;
std::deque<Writer*>::iterator iter = writers_.begin();
++iter;  // Advance past "first"
for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
        // Do not include a sync write into a batch handled by a non-sync write.
        break;
    }

    if (w->batch != nullptr) {
        size += WriteBatchInternal::ByteSize(w->batch);
        if (size > max_size) {
            // Do not make batch too big
            break;
        }

        // Append to *result
        if (result == first->batch) {
            // Switch to temporary batch instead of disturbing caller's batch
            result = tmp_batch_;
            assert(WriteBatchInternal::Count(result) == 0);
            WriteBatchInternal::Append(result, first->batch);
        }
        WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
}
```

接下来就会顺序遍历等待队列，将遇到的`Writer`合并到当前的`WriteBatch`，`result`中，只有三种情况会终止合并：

1. 当前的`Writer`要求`sync`，而第一个任务不要求`sync`；
2. `result`的`size`超过`max_size`；
3. `writers_`等待队列遍历完成；

合并实质上就是调用`Append`方法进行追加即可。



## 3.5 写操作总结

在了解写操作的执行过程后，我们发现，用户可以在外部并行的调用`db->Write`方法，每个`db->Write`调用都被记录为一个`Writer`，`Writer`会被加入到一个队列中执行。

执行时，会先确认`MemTable`是否有空间写，在确认过程中会兼顾`Level0`的文件数量以及`imm_`的状态。有空间写后，会对`Writer`进行顺序合并，合并的结果是一个`WriteBatch`。执行时会执行合并后的`WriteBatch`。

执行后会将被合并的`Writer`全部标记为完成，并拉起。如果队列中有待执行的`Writer`，那么会将待执行`Writer`对应的线程拉起，以达到继续执行的目的。



# 4-总结

这篇文章从`leveldb`的外部接口入手，明确了所有的修改操作实质上都是调用了`Write`方法,执行`WriteBatch`中指定的修改。

随后介绍了`WriteBatch`提供的方法，并且介绍了`WriteBatch`内部的数据结构。

最后介绍了`Write`方法接收到用户提供的`WriteBatch`后的处理流程。



但是还有很多问题值得思考，但是由于篇幅原因，我们不再展开讨论：

1. `WriteBatch`为什么要手动维护一个`string`呢，用`vector`不可以么？

2. 多线程操作`Write`过程中的线程安全如何保证，哪些部分是临界区？
3. 为什么写日志、内存时可以解锁？
4. 等等等等



# 5-参考资料

1. [LevelDB源码解析(6) 写任务(WriterBatch)合并](https://www.huliujia.com/blog/cc155a0319/)
2. [leveldb源码剖析DBImpl::MakeRoomForWrite函数的实现](https://blog.csdn.net/swartz2015/article/details/66972106)
3. [LevelDB 源码分析「三、高性能写操作」](https://sf-zhou.github.io/leveldb/leveldb_03_write_batch_and_log.html)
4. [深入浅出leveldb之高性能中锁的使用](https://blog.csdn.net/weixin_42663840/article/details/82629473)

