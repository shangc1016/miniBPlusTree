### sqlet



part10   


在上一小节中，插入数据只能在同一个leaf node中，也就是说，数据长度只能在一个节点，因为在插入数据的时候，没有实现叶子结点的分裂。

```c
// 插入数据Execute_insert首先调用table_find找到合适的插入位置
// 但是在table_find中，如果在root_node中没有找到合适的位置，直接返回错误。这就是本小节的工作。

Cursor *table_find(Table *table, uint32_t key) {
  uint32_t root_page_num = table->root_page_num;
  // 数据库的node，一个页面
  void *root_page = get_page(table->pager, root_page_num);
  // 判断这个nod而必须是叶子结点，只有叶子结点存储完整数据
  if (get_node_type(root_page) == NODE_LEAF) {
    // 在这个node中根据key值查找记录，返回游标
    return leaf_node_find(table, root_page_num, key);
  } else {
    printf("Need to implement searching an internal node\n");
    exit(EXIT_FAILURE);
  }
}
```



在这儿可以看到，在本小节中。只支持两个叶子结点，一个根节点的B树。如果原来的节点不是叶子结点，直接返回错误。

```c
// 叶子结点的分裂算法
void leaf_node_split_and_insert(Cursor *cursor, uint32_t key, Row *value) {
  // 1、分配一个新的叶子结点，把一般的数据拷贝到新的叶子节点中
  // 2、把当前的K-V记录,插入新旧两个叶子节点中的一个
  // 3、更新两个叶子节点的父子关系

  void *old_node = get_page(cursor->table->pager, cursor->page_num);
  // 直接返回pager的page数量，此时pager中这个page未分配，就会在get_page中分配新的页面
  uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
  void *new_node = get_page(cursor->table->pager, new_page_num);
  // 初始化叶子结点，
  initialize_leaf_node(new_node);

  // 需要考虑的数据包括这个叶子结点的全部数据以及即将插入的这个数据
  // 要把这些数据分成两部分，其中的一部分拷贝到新的叶子节点上
  for (uint32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
    void *destination_node;
    // 把一个page中，处于高key的一半记录拷贝到新的页面page
    if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
      destination_node = new_node;
    } else {
      destination_node = old_node;
    }
    // index_within_node就是这条记录在新的节点中的位置，余LEAF_NODE_LEFT_SPLIT_COUNT就行
    uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
    void *destination = leaf_node_cell(destination_node, index_within_node);

    // 下面这么memcpy的话，岂不是old_page的也要拷贝一遍？
    if (i == cursor->cell_num) {
      // 如果这条数据是即将插入的数据，直接序列化到指定位置
      serialize_row(value, destination);
    } else if (i > cursor->cell_num) {
      // 后半段数据?
      memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
    } else {
      // 前半段数据?
      memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
    }
  }

  // 更新每个node中的数据记录条数
  // 分别设置原来的node以及新的node上面的数据记录的条数
  *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
  *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

  // 然后更新两个叶子结点的父子关系
  // 如果old_node是根节点，没有父节点；那就需要创建一个父节点，
  if (is_node_root(old_node)) {
    return create_new_root(cursor->table, new_page_num);
  } else {
    // TODO：非根节点的情况暂不考虑
    printf("Need to implement updating parent after split\n");
    exit(EXIT_FAILURE);
  }
}
```


只有叶子结点存储数据。目前，只能支持插入硬编码k-v的记录，(id:int, username:string, email:string)。  
一个叶子结点除过元数据之外，只能存储13条数据，那么在插入第14条记录的时候，就会引起叶子结点的分裂。首先分配一个新的叶子结点，然后把原来的13条记录以及新插入的一条记录，总共14条。平均分为两半，偏移靠后的一般被拷贝到新的叶子结点的前半部分。  
也就是说原来只有一个old_page(叶子结点，根节点)，新分配了一个new_page(叶子结点、非根节点)。拷贝完成之后，重新建立节点之间的关系，新分配一个节点叫left_child，把old_page也就是root_page拷贝过去。

                    root_page(NODE_INTERNAL、ROOT_NODE)
                        /                \
                      /                    \
                    /                        \
              left_child(NODE_LEAF)         right_child(NODE_LEAF)

最后设置号左右两个孩子节点的记录条数，然后设置好talbe->root_page_num为root节点就行(这个其实不用设置，因为left_child中的数据是从root拷贝过去的，talbe->root_page_num不需要变)  




在本小节中，虽然可以实现叶子结点的分裂了，但是在叶子结点分裂之后，没有实现在NODE_INTERNAL内部节点上的插入。而且如果原来的一个叶子结点的key是逐个递增的(1->14)。分裂之后左孩子(0->7)，右孩子(8->14)，但是因为key不能重复，所以两个叶子节点中间的部分是不能插入数据的。   
而且，另一个大问题就是，现在只是实现了一个叶子结点扩展成两个叶子节点(见上图)，但是更进一步的B树节点分裂还不行。  
