### sqlet



##### part8  

在之前的章节中，一个表存储在文件中，而且是逐行存储的。  
在本节，改用B树实现表的存储。  


节点分为两种，internal和leaf节点  

本节首先关注leaf node，leaf node需要存储kv数据，所以leaf node中需要额外的字段来确定一个叶子结点中有多少条数据，就是LEAF_NODE_NUM_CELLS_SIZE，大小是sizeof(uint32_t)。  

一个节点大小是4K，叶子结点剩余的部分就是用来存储每条记录的。  


到此位置，在本节中实现了使用pager的抽象，以及使用B树的leaf节点，但是在一个leafnode中，数据仍然是按照插入的顺序排序的。  
