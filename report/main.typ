#import "template.typ": *

#show: project.with(
  theme: "lab",
  title: "数据库系统 实验",
  course: "数据库系统",
  name: "MiniSQL",
  author: "陈皓天 张远帆",

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