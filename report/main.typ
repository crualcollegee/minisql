#import "template.typ": *

#show: project.with(
  theme: "lab",
  title: "数据库系统 实验",
  course: "数据库系统",
  name: "MiniSQL",
  author: "陈皓天",

  place: "西教 506",
  teacher: "苗晓烨",
  date: "2025/04/19",
)



= 第一章 #h(0.4em) 模块 #h(0.2em)1
#h(2em)该模块实现了磁盘数据页管理、位图页、缓冲池管理和LRU 替换策略，并加以必要的并发控制锁。其中，磁盘数据页管理和缓冲池管理的变量较多，逻辑较为复杂，我通过绘画思维导图的方式帮助自己完成代码。

*1. 磁盘数据页管理*

#h(2em)以```c AllocatePage```为例，虽然这是个磁盘数据页管理类，但他一开始并不直接与磁盘相交互，而是先将```c disk_file_meta_page```从内存中读取信息，然后根据需要创建特定索引的Bitpage类，然后从磁盘读入这个类，经过修改后再写回磁盘.我绘制的思维导图图如下：

#figure(
    grid(columns: 37em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/1.png")
]))
#h(2em)可以看到，```c Disk_Management```直接与```c disk_file_meta_page```和```c bitmap_page```交互，来管理磁盘page的分配与释放; 在Allocate中，物理地址和逻辑地址的转换尤为重要，其中```c locate_Bitmap```是我命名的来索引extent的变量，当确定某一个```c locate_Bitmap```的值之后，就需要转化为物理地址，来传入```c ReadPhysicalPage```函数，于是需要以下逻辑：

\

#align(center)[```cpp int locate_physics = 1 + (1 + BITMAP_SIZE) * locate_Bitmap;```]

\
#h(2em)这里考虑到了 实际存储数据页 和 meta_page 占用的页数，直接得到了```c bitmap_page```的物理索引

\

*2. 位图页*

#h(2em)此处的代码较为简单，在上面的磁盘数据管理的逻辑中，一个extent包含着一个位图页和多个数据存储页，位图页的目的是为了管理和快速索引数据存储页。

#h(2em)值得注意的是，这个类中的```c next_free_page_```变量记录了下一个空闲的页数索引（从0开始）,有助于查找空闲页的速度.

#h(2em)bytes数组的每一个位的长度是8个bits,通过```c byte_index```c和```c bit_index```的位操作可以得出该页是否空闲的信息

\

*3. 缓冲池管理*

#h(2em)此模块变量较多，我同样通过mind map的方式帮助自己记忆
#figure(
    grid(columns: 39em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/2.png")
]))
#h(2em)这个缓冲池管理类和刚刚的磁盘数据页管理类的区别在于，缓冲池管理类是包含真的数据的。实际上其数据存储在一个Page\* 数组中，Page\* 数组的索引是frame_id;相当于这个Page数组包含着磁盘中的某几页数据，通过page_table\_将磁盘页和frame页相对应类起来；在必要的时候可以通过LRU策略替换其中的某些磁盘页。 

\

*4. LRU替换策略*

#h(2em)```c LRUReplacer```类提供了保存可替换frame的容器，用于在缓冲池```c FetchPage```方法和```c NewPage ```方法时，当找不到空闲页的时候选择last recently used页进行替换，并且当其调用 ```cpp Pin```方法的时候将对应frame移出容器；

#h(2em)这里一共有两个容器来保存可替换frame,分别是```cpp list<frame_id_t> lru_queue```,用作记录顺序，以及```cpp unordered_set<frame_id_t> lru_set;```,用作快速查找；两者结合，lru方法的效率可以得到大幅提升.

\

*5. 正确性测试*

#h(2em)分别对上述四个模块进行三个测试，均通过。

#figure(
    grid(columns: 39em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/3.png")
]))

以及我的git推送记录
#figure(
    grid(columns: 39em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/4.png")
]))

=  第二章 #h(0.4em) 模块 #h(0.2em)3
#h(2em)IndexManagement 模块的代码量较大，我花费了大约一周时间完成了这个代码，并且通过了测试；模块2是交给我的队友完成的，模块2与模块3的交集大概就是在 ```c row.cpp 和 generickey``` 上了；我首先读懂了这两个模块的代码，然后从底层代码开始看起。

*1. 基本page类*

#h(2em)首先是完成了 ```c b_plus_tree_page.cpp ```代码，这个模块较为简单，是叶子节点和内部节点的父类，就是完成了一些两者共有的方法，比较简单，不加以赘述.

#h(2em)接着我完成了```c b_plus_tree_internal_page.cpp```代码；首先要明确的是，一个internal类在一般情况下分为多个genericKey类和page_id_t 类，这两个类合在一起为一个pair；但其中的地一个pair一般只包含page_id_t 类，这是b+树特有的性质。

#h(2em)这个模块给我带来印象最深的就是多个 internalpage 类的交互，通过外界参数recipient的传入，我们可以将this的部分pair通过类的public接口转移到recipient去，实现了传递。

#figure(
    grid(columns: 39em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/5.png")
]))

#h(2em)但做到这里为止，我发现internalpage类有些接口没有被自己使用到，而且接口繁多，我猜测是有其他类将来会引用这些接口

\

#h(2em)然后便是leafpage类的完成，这个类和internalpage类就较为重合了，我较为轻松的完成了；我特别注意到了删除某个pair这个方法的时候，我没有使用PairCopy进行批量“位移”，因为担心内存重叠的危险，我按照类似冒泡排序中的逐个位移的方法，一次只PairCopy 1个pair.

#figure(
    grid(columns: 39em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/6.png")
]))
#h(2em)特别需要注意的是，这个pair包含的是一个```c generickey ```和 ```c rowID```的指针，也就是说包含类一个指向实际数据（rowID）的指针。而刚刚的 internalpage 的pair的第二个元素就是 一个pageID（不是指针）,可以调用```c FetchPage() ```来获取相应```c pageID```的page.

#h(2em)（在以前```（ADS课程）```的实现中内部节点往往是通过指针指向子节点的，而这里有```c buffer_pool_manager.cpp```充当了管理页面的作用）

\

*2. b_plus_tree树类*

#h(2em)然后就是工程量最大的```c b_plus_tree.cpp```的编写，也就是这个地方才会和```c buffer_pool_manager.cpp```打交道，尤其是其```c UnpinPage()``` 函数，要被经常调用；一开始我写的时候经常忘了这茬,但后来对读取页面和保存页面的操作熟悉之后就得心应手了。

#h(2em)这个类其实就是一棵树，但类不包含树的所有信息，而是在需要的时候从内存池中fetch下来。其实我*有个疑惑*，就是这个类对
```c buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)) ```这个记录 ```c 树的index和root_page_id``` 的页面经常调用和修改，但从来没看见被使用过。我只能合理猜测在后面的模块中会被用到吧。（树的index在这个模块里也没被使用过）

#h(2em)其他的模块对我印象较深的就是删除的向上递归过程，以及```c CoalesceOrRedistribute()```、```c Coalesce()``` 和 ```c Redistribute()```的相互调用，显得比较有逻辑感。这几个函数也调用了``` c (internal/leaf) page```类大量的 ```c movehalfto()```、```c moveallto()```等之前看起来比较奇怪的函数。

#figure(
    grid(columns: 39em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/7.png")
]))

*3. index_iterator类*

#h(2em)这个类比较简单，实现了一个简单的迭代器iterator,具体到某一页的某一个pair.另外，这个iterator也在```c b_plus_tree``` 中被使用到了,在此不加以赘述

#figure(
    grid(columns: 28em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/8.png")
]))

*4. 正确性测试*
#figure(
    grid(columns: 26em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/9.png")
]))

*5. git推送记录*

#figure(
    grid(columns: 33em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/10.png")
]))

= 第三章 #h(1em) 模块5
#h(2em)本模块分为两个部分，planner和executor；planner负责将用户输入的sql语句转化成规范化的，类封装的ast树；而executor则是根据ast树执行相应的操作。这个模块运用到了大量的```c catalog.cpp ``` 函数，以及```c syntax_tree.h 和 derr.h```内容，所以我花费了一定时间去了解了外部的一些定义。

#h(2em)实际要完成的工作就是```c execute_engine.cpp```的部分内容，其开头的executor调用和```c select()```方法实现已经完成了,我们实际上只需要完成部分executor的实现即可。

#h(2em)对于```c CreateTable```方法，其具体的实现其实不难，但要理解它ast树的逻辑有点困难；为此，我运行了bin目录下的main，在同目录下观察syntax.txt画出了以下逻辑图

#figure(
    grid(columns: 35em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/11.png")
]))

#h(2em)根据逻辑图，我能较为轻松的完成这个函数的设计，但要注意以下逻辑：1.当```c primary key ```只有一个元素的时候，那么这个元素一定是```c unique key ``` 2. ```c primary key 和 unique key```都要调用```c CreateIndex()```函数来形成```c b+```树索引.

#h(2em)剩余几个函数都较为简单，```c ExecuteDropTable()```方法用到了```c catalog.cpp```中```c GetTableIndexes() DropIndex() DropTable()```多个函数，需要花费一定时间理解

\

*2.正确性测试*

#h(2em)```c executorTest```并不能测试到我要实现的代码，即对表的一些无关乎数据的操作；如果要测试这部分，需要我运行/bin目录下的main进行手动命令行交互：

#figure(
    grid(columns: 35em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/12.png")
]))
#figure(
    grid(columns: 35em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/13.png")
]))

#figure(
    grid(columns: 35em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/14.png")
]))
#figure(
    grid(columns: 35em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/15.png")
]))
\
\
\
#h(2em)当然，```c executorTest```可以验证其他一些模块例如```c catalog.cpp```不会出错：
\
\
#figure(
    grid(columns: 35em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/16.png")
]))
\
*3.git推送记录*
\
\
#figure(
    grid(columns: 35em, inset: 0.1em,fill: luma(230),align(right)[
      
  #image("images/17.png")
]))