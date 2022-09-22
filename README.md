### sqlet



part12


在本小节，实现select语句查询整个数据库，在part11中，select数据只能得到一个叶子结点的数据，这部分实现遍历整个B+树。


在本节中，通过修改table_start函数，使得游标指向第一个叶子结点，然后测试只能打印第一个叶子结点的7条记录，

```c
// 创建cursor指向table头
Cursor *table_start(Table *table) {
  // 让cursor指向key为0的一条记录
  // key不能小于0，所以0肯定是表的第一行
  Cursor *cursor = table_find(table, 0);

  // 拿到游标指向的那一个页面，肯定就是第一页
  void *node = get_page(table->pager, cursor->page_num);
  // 计算得到第一个叶子结点的记录条数
  uint32_t num_cells = *leaf_node_num_cells(node);
  cursor->end_of_table = (num_cells == 0);
  return cursor;
}
```

为了能实现叶子结点可以串起来，在叶子结点的page中增加一个字段next_leaf指针，指向下一块叶子结点、

```

            root_node
             /     \
            /       \
           /         \
          /           \
         /             \
    leaf1   ------>   leaf2
           (next_leaf)
```



          
           





































------

pager这个抽象结构：

输入：页号
输出：得到物理页面
