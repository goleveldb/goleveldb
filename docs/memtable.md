# GoLevelDB —— Memtable

---

- `auther`: jaegerwang

---

在本章节中我们讲解`Memtable`的设计与实现。

## Wath is `Memtable` ?

`Memtable`是写入磁盘前的缓存表，在`levelDB`中一个`DB`实例会维护两个`Memtable`：

- `mem`：内存表，用于缓存写入的数据；
- `imm`：只读内存表；

最新写入的数据会被写入到`mem`中，当`mem`写满后，将`mem`置为`imm`，并开始将`imm`同步到磁盘。



基于上面的情况，我们的`memtable`只需要有以下三个接口：

- **`Insert`**：插入一条`key-value`数据；
- **`Get`**：通过`key`获取对应的`value`；
- **`Iterator`**：生成内存表迭代器，用于遍历内存表；

内存表中数据的删除操作，通过特殊的**`Insert`**操作完成，这个问题我们接下来会继续讨论。

## Records in `Memtable`.

接下来讨论每条数据如何在内存表中存储的问题。

在上面，我们说了`Memtable`存储的是`Key-Value`数据，但实质上，`Key`与`Value`不会分开存储，而是会整合成一条`Record`进行存储，这个`Record`的整合方法如下：

<img src="https://goleveldb-1301596189.cos.ap-guangzhou.myqcloud.com/record_fmt.png" style="zoom:50%;" />

其中：

- `KeyLength`：`Key`部分的字节数；
- `KeyData`：`Key`部分的数据；
- `sequenceNumber`：序列号（全局唯一）；
- `valueType`：记录类型，有`typeDelete`, `typeData`两种，为`typeDelete`时，这条记录无效；
- `ValueLength`：`Value`部分的字节数；
- `ValueData`：`Value`部分的数据；

### `Varint`编码

在上面我们可以看到，`KeyLength`, `ValueLenght`是通过`Varint`进行编码的，这里我们简单讲一下什么是`Varint`以及用它有什么好处；

#### 传统 int 编码存在的问题

首先，`Varint`的编码方式非常简单，我们以一个`32 bits`数字为例：

在日常场景中，`32 bits`能够表示的范围是`0 ~ 4294967295`，需要消耗`4 bytes`。

- 可以看出，传统的编码方式存在缺陷**：我们大多数时候需要表示的数字只需要两个`bytes`就可以表示清楚，空间浪费很多！**

#### varint 编码解决问题的方法

如果我们读数字的时候能一个`byte`一个`byte`的读，读到数字结束就不读了，那就能达到：

- 节约空间；
- 表示原本能够表示的范围；

这两个要求了！

`Varint`用每一个`byte`的最高位表示当前`byte`是否是这个数字的最后一`byte`，这样就可以达到上面说的效果了。

这种方法在小数字高频出现，大数字低频出现的场景中非常有效；在golang中，只需要使用：`binarys.Varint`方法就可以实现了；

### `sequenceNumber` & `valueType`字段意义

- `sequenceNumber`不由`Memtable`维护，而是由上层模块传入，该数字全局唯一，随每次插入递增，相当于序列号；

- `valueType`是用于标记数据是否有效的`byte`；



到此为止，我们已经了解了`Memtable`中每条记录长什么样子了；

## `Memtable` 底层数据结构

 我们的`Memtable`需要进行`Insert`和`Get`操作，并且充当写磁盘的缓存，因此`Memtable`需要较高的性能。

目前`Insert` & `Get`操作比较快的数据结构有：

- 红黑树：读写稳定`O(logN)`。[我自己的实现](https://github.com/wang-sy/rbtree)
- `skiplist`：在玄学加持下，读写理论`O(logN)`。

但是由于红黑树的插入操作需要进行非常复杂的分类讨论，现在的很多开源项目（`redis`, `leveldb`等）都选择使用`skiplist`作为`KV`存储的底层数据结构；



### `skiplist` 基本原理

<img src="skiplist_struct.png" style="zoom:67%;" />

#### skiplist的特性

从图的最左侧看起，这个图展示了一个`MaxLevel = 4`的`skiplist`，它是由四层链表组成的。

`skiplist`的特性可以从两个角度来看：

- 在每一层内从左往右依次递增；
- 从下向上，每一层的元素个数依次递减；

#### skiplist的查找

我们来看一下在这样的结构下，应该如何查找一个特定的元素：

假设我们需要查询`9`：

- 我们从`header`开始，从高层向下层开始遍历；

- 首先我们从最高层开始，能够找到的第一个数字是7，这个数字小于9，所以我们当前节点移动到`7`；
- 由于`L3`的下一个节点是空节点，所以不再继续向前，而是查找`L2`的下一个节点；
- 由于`L2`的下一个是10，大于我们想要查找的目标，所以也不再继续向前；
- 经过多轮降低`Level`后，最终在`L0`找到9；

之所以这样的查找逻辑成立，是因为：对于同一位置的`next`节点，层数越高，`next`节点的值一定越大，在第一层找到最后一个节点后，降低`Level`的实质是对区间进行进一步缩小。

#### skiplist的插入

看懂了查找后，插入操作就非常简单了，插入只需要使得添加元素后的`skiplist`满足原有性质即可。

插入值为12的元素时，具体的操作方法如下：

- 随机一个插入元素的层数，在`0~3`之间；
- 找到需要插入元素的位置（在这里是在10之后，在15之前）；
- 对`0~level`之间的层数，分别执行链表的插入操作即可；

## `Memtable` 中完成CRUD

通过上面的学习，我们知道`Memtable`通过封装`skiplist`以维护`Key-Value`数据对；在这一节中我们学习`leveldb`如何通过调整数据顺序完成内存表中数据的增删改查操作；

### `Memtable` 面对的场景

- `Memtable`是写磁盘前的缓存；
- `Memtable`只有`Insert`操作，没有`Update`，`Delete`操作，但是有需要实现这样一套功能；

### `Memtable` 解决问题的方法

#### 从skiplist的排序入手

我们已经知道：

- `Memtable`通过`skiplist`进行数据存储
- `skiplist`中的一条数据是`key/sequnceNumber/valueType/value`组成的；

那么事实上我们可以从`skiplist`操作时的排序方法入手来解决上面的问题；

#### 构建的排序方法

我们希望：

- 对于相同的`Key`，使用拥有最新（也就是最大）`sequnceNumber`的那条记录，这样就能够完使得每次取出的都是对于该`key`最后一次操作后的结果；
- 取出一条记录后，`Memtable`能够判断该数据是否被删除；

排序的方法：

- 先取出`userkey`进行排序；
- 对于`userkey`相同的，通过`sequnceNumber`排序；

附加的方法：

- 当`valueType`为`typeDelete`时，表示该数据已经被删除；

经过上面的规则，最终的`skiplist`中顺序排放的元素结构如下：

<img src="https://goleveldb-1301596189.cos.ap-guangzhou.myqcloud.com/nodes_in_memtable.png" style="zoom:50%;" />

- 绿色的`key1`节点的`seqNumber`较大，通过`key1`查询时，只会查询到绿色的节点，而不会查询到灰色的节点，这就实现了修改的操作；
- 红色的节点的序列号也较大，同时他的`type`位被标记为`Delete`，所以在解析时会被认为已删除；

## `Iterator` 迭代器

最后，我们来了解`Memtable`的迭代器，这个没啥好讲的，迭代器迭代的对象是`skiplist`，但是由于存储对象相同，感觉迭代的是`Memtable`；

`iterator`有以下操作：

- `next`：直接在`L0`访问下一节点实现；
- `prev`：由于`skiplist`不是双向链表，需要进行一次搜索，搜索比当前节点小的节点即可；
- `seek`：找到大于等于`key`的节点，通过`skiplist`实现；

还有一些其他操作，都不重要了；



## 代码: http://www.github.com/goleveldb/goleveldb

