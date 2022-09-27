#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct {
  char *buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_UNRECOGNIZED_STATEMENT,
  PREPARE_SYNTAX_ERROR,
  PREPARE_STRING_TOO_LONG,
  PREPARE_NEGATIVE_ID,
} PrepareResult;

typedef enum {
  EXECUTE_SUCCESS,
  EXECUTE_DUPLICATE_KEY,
  EXECUTE_TABLE_FULL,
} ExecuteResult;

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

// 这个是硬编码的这张表的字段
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
  char email[COLUMN_EMAIL_SIZE + 1];
} Row;
// table hard-coded end

// 硬编码的数据库表，计算各个字段的size以及offset，从而在序列化，反序列化的时候计算各个字段的位置
// 以及确定一个page里面可以放多少个row
#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

// 数据库表位置
const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100

typedef struct {
  int file_descriptor;
  uint32_t file_length;  // 文件的大小，可以计算出表中的数据有多少行
  uint32_t num_pages;    // 底层B+树 数据结构中的叶节点数
  void *pages[TABLE_MAX_PAGES];  // pages就是从文件读到内存中的一个page缓存；
} Pager;
// Pager 是一个数据库表B+树的一个缓存，是一个指针数组

// 这个就是数据库表的page指针数组，包括了表的行数；
typedef struct {
  Pager *pager;
  uint32_t root_page_num;  // 这个可以确定一个B+树
} Table;

typedef struct {
  StatementType type;
  Row row_to_insert;  // 这个是插入语句
} Statement;

// cursor游标
typedef struct {
  Table *table;       // 根据cursor可以找到它指向了哪个表
  uint32_t page_num;  // page_num表示B+树上的哪个节点node
  uint32_t cell_num;  // 表示一个node上的第几条数据
  bool end_of_table;  // 表示到达表的末尾，新插入的数据就放在这个位置
} Cursor;

// part8
// 分为内部节点以及叶子节点
typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;
// common node header layout；内部节点（包括根节点）
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);  // 区别内部节点以及叶子结点
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);  // 是否是根节点
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);  // 父指针
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// leaf node header layout；叶子结点
// 可以看到叶子结点比内部节点多了一个cells属性；
const uint32_t LEAF_NODE_NUM_CELLS_SIZE =
    sizeof(uint32_t);  // 叶节点cells，就是一个node存储的数据条数
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
// const uint32_t LEAF_NODE_HEADER_SIZE =
//     COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;
// 指向下一个叶子结点的指针
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET =
    LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NEXT_LEAF_SIZE;
// 叶子结点包含两个而外的元数据：本节点的记录条数、指向下一个叶子结点的指针
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                       LEAF_NODE_NUM_CELLS_SIZE +
                                       LEAF_NODE_NEXT_LEAF_SIZE;

// leaf node body layout
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
// part8 end
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
// LEAF_NODE_LEFT_SPLIT_COUNT这个就是英特叶子节点中记录条数的一半
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

// internal node header layout
// 一个字节存储内部节点保存的key值
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;

const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
// internal_node_right_child_size 保存了内部节点的最右侧的子节点的页号
// 可以加速寻找子节点，如果要找内部节点的最后一个子节点，不用遍历了。
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                           INTERNAL_NODE_NUM_KEYS_SIZE +
                                           INTERNAL_NODE_RIGHT_CHILD_SIZE;

const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE =
    INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

// 设置内部节点最多有三个子节点?为啥设置这么少
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;

const uint32_t INTERNAL_NODE_SPACE_FOR_CELLS =
    PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_CELLS_NUM =
    INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE;

// num_keys和right_child合起来是internal_node的元数据
// 真正指向叶子结点的指针是internal_node_cell，每个cell包含一个key、一个指向子节点的指针
// INTERNAL_NODE_HEADER_SIZE大小：14B，INTERNAL_NODE_CELL_SIZE大小：8B，
// 一个节点node大小4KB，可以分配的cell数量是：(4096-14) / 8 = 510。
// 也就是说，一个内部节点最多有510个叶子结点

// FIXME：这儿的right_child指针是啥意思？？？

/////////// 关于叶子结点、内部节点的定义，为啥定义常量而不用结构体呢？

// function declaration =============================
// 序列化、反序列化
void serialize_row(Row *, void *);
void deserialize_row(void *, Row *);

void db_close(Table *);
Table *db_open(const char *);

void *get_page(Pager *, uint32_t);
// void *row_slot(Table *, uint32_t);
Pager *pager_open(const char *);
void page_flush(Pager *, uint32_t);

InputBuffer *new_input_buffer();
void print_prompt();
void read_input(InputBuffer *);
void close_input_buffer(InputBuffer *);
void print_row(Row *);

MetaCommandResult do_meta_command(InputBuffer *, Table *);
PrepareResult prepare_insert(InputBuffer *, Statement *);
PrepareResult prepare_statement(InputBuffer *, Statement *);
ExecuteResult execute_select(Statement *, Table *);
ExecuteResult execute_insert(Statement *, Table *);
ExecuteResult execute_statement(Statement *, Table *);

void cursor_advance(Cursor *);
void *cursor_value(Cursor *);

Cursor *table_start(Table *);
Cursor *table_end(Table *);
// B+树中的一个node节点，相关的操作函数
uint32_t *leaf_node_num_cells(void *);
void *leaf_node_cell(void *, uint32_t);
void *leaf_node_value(void *, uint32_t);
void initialize_leaf_node(void *);
void print_constants();
void leaf_node_insert(Cursor *, uint32_t, Row *);
void print_leaf_node(void *);

Cursor *leaf_node_find(Table *, uint32_t, uint32_t);
Cursor *table_find(Table *, uint32_t);
NodeType get_node_type(void *);
void set_node_type(void *, NodeType);
void leaf_node_split_and_insert(Cursor *, uint32_t, Row *);

uint32_t *internal_node_num_keys(void *node);
uint32_t *internal_node_right_child(void *node);
uint32_t *internal_node_cell(void *node, uint32_t cell_num);
uint32_t *internal_node_child(void *node, uint32_t child_num);
uint32_t *internal_node_key(void *node, uint32_t key_num);
uint32_t get_node_max_key(void *node);
bool is_node_root(void *node);
void set_node_root(void *node, bool is_root);
void indent(uint32_t level);
void print_tree(Pager *pager, uint32_t page_num, uint32_t indentation_level);
uint32_t get_unused_page_num(Pager *pager);
void create_new_root(Table *table, uint32_t right_child_page_num);

uint32_t internal_node_find_child(void *node, uint32_t key);
// function declaration end =========================

// part8，访问key、value以及元数据都需要用到上面定义好的宏，以及指针算术运算
// 返回这个node节点的cell数据条数，根据指针运算，node地址加上CELLS的偏移
uint32_t *leaf_node_next_leaf(void *node) {
  return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

uint32_t *leaf_node_num_cells(void *node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}
// 返回一个node中的第几条记录
void *leaf_node_cell(void *node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}
// 返回一个node中的第cell_nums条记录的key值，
uint32_t *leaf_node_key(void *node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num);
}
// 返回一个node中第cell_num条记录的value值，
void *leaf_node_value(void *node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}
// 初始化一个node，这儿直接num_cells设置为0
void initialize_leaf_node(void *node) {
  *leaf_node_num_cells(node) = 0;
  // 设置节点，不为根节点
  set_node_root(node, false);
  set_node_type(node, NODE_LEAF);
  // 设置下一个叶子结点的指针为0
  *leaf_node_next_leaf(node) = 0;
}
void initialize_internal_node(void *node) {
  set_node_type(node, NODE_INTERNAL);
  // 内部节点初始化为非root
  set_node_root(node, false);
  *internal_node_num_keys(node) = 0;
}

// 打印B+树相关的元数据字段的大小
void print_constants() {
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS :%d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODEMAX_CELLS :%d\n", LEAF_NODE_MAX_CELLS);
}
//打印处一个叶子结点的记录条数、以及每个记录的key(对应到硬编码的单表数据库中，就是id字段)
// void print_leaf_node(void *node) {
//   // 得到这个节点的记录数目
//   uint32_t num_cells = *leaf_node_num_cells(node);
//   printf("leaf (size %d)\n", num_cells);
//   for (uint32_t i = 0; i < num_cells; i++) {
//     uint32_t key = *leaf_node_key(node, i);
//     printf("  - %d : %d\n", i, key);
//   }
// }

// 原来的数据结构是数组指针的方式，现在是数据结构是树。
// 存储结构可视化，需要递归的遍历树

// 根据所在的树的层数，打印空格，表示层级关系
void indent(uint32_t level) {
  for (uint32_t i = 0; i < level; i++) {
    printf("  ");
  }
}
// part10中，只能指支持一个内部节点，两个叶子结点；
// 在本节中，以及支持插入的时候多个内部节点了，
// 这儿需要改进
void print_tree(Pager *pager, uint32_t page_num, uint32_t indentation_level) {
  void *node = get_page(pager, page_num);
  uint32_t num_keys, child;

  switch (get_node_type(node)) {
    case (NODE_LEAF):
      // 得到叶子结点的数据个数
      num_keys = *leaf_node_num_cells(node);
      // 打印缩进
      indent(indentation_level);
      printf("- leaf (size %d)\n", num_keys);
      // 依次打印叶子节点中所有数据的key
      for (uint32_t i = 0; i < num_keys; i++) {
        indent(indentation_level + 1);
        printf("- %d\n", *leaf_node_key(node, i));
      }
      break;
    case (NODE_INTERNAL):
      num_keys = *internal_node_num_keys(node);
      indent(indentation_level);
      printf("- internal (size %d)\n", num_keys);
      for (uint32_t i = 0; i < num_keys; i++) {
        child = *internal_node_child(node, i);
        // 递归
        print_tree(pager, child, indentation_level + 1);
        indent(indentation_level + 1);
        printf("- key %d\n", *internal_node_key(node, i));
      }
      // 在这儿可以看到，内部节点的其他子节点和最右侧的子节点是分开的
      child = *internal_node_right_child(node);
      print_tree(pager, child, indentation_level + 1);
      break;
  }
}

// part8 end

// part9
// 根据node地址计算得到node的类型
NodeType get_node_type(void *node) {
  uint8_t value = *((uint8_t *)(node + NODE_TYPE_OFFSET));
  return (NodeType)value;
}

void set_node_type(void *node, NodeType type) {
  uint8_t value = type;
  *((uint8_t *)(node + NODE_TYPE_OFFSET)) = value;
}
// aprt9 end

// 一行数据的存取，序列化
void serialize_row(Row *source, void *destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

// 反序列化
void deserialize_row(void *source, Row *destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

// 返回pager中的第几页地址
void *get_page(Pager *pager, uint32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds. %d -> %d\n", page_num,
           TABLE_MAX_PAGES);
    exit(EXIT_FAILURE);
  }
  // 这一页不在pager缓存中，cache没有命中
  if (pager->pages[page_num] == NULL) {
    // 先分配一页，放从文件读到的内容
    void *page = malloc(PAGE_SIZE);
    // 计算整个文件有多少页page
    uint32_t num_pages = pager->file_length / PAGE_SIZE;
    // 有可能文件有不足一页的，也加上，
    if (pager->file_length % PAGE_SIZE) {
      num_pages += 1;
    }
    // 要读的page是一个有效的page
    if (page_num <= num_pages) {
      //先把文件指针放到要读的那一页
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("Error reading file: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }

    // 从文件中读到了page中，更新缓存
    pager->pages[page_num] = page;

    // TODO，这是啥意思？
    if (page_num >= pager->num_pages) {
      pager->num_pages = page_num + 1;
    }
  }
  // cache命中，直接返回那一页的地址；
  // 只有成功之后更新了cache，否则其他返回都是NULL
  return pager->pages[page_num];
}

// 返回游标cursor所在的位置的一条记录
void *cursor_value(Cursor *cursor) {
  // 不再直接使用pager索引数据库表的page地址，而是根据cursor检索

  // page_num: 当前游标所在的page
  uint32_t page_num = cursor->page_num;
  // 返回page的地址
  void *page = get_page(cursor->table->pager, page_num);

  // page地址加上cell_num所对应的地址偏移
  return leaf_node_value(page, cursor->cell_num);
}

// cursor往下移动一条数据
void cursor_advance(Cursor *cursor) {
  // 得到cursor所在的当前page
  uint32_t page_num = cursor->page_num;
  void *node = get_page(cursor->table->pager, page_num);

  cursor->cell_num +=1;
  // cursor到达节点的最后，跳转到新的叶子结点
  if(cursor->cell_num >= (*leaf_node_num_cells(node))){
    // 设置cursor到下一个叶子结点
    uint32_t next_page_num = *leaf_node_next_leaf(node);
    if (next_page_num != 0) {
      // 如果还有下一个叶子结点，那么直接设置cursor的页号为下一个叶子结点
      cursor->page_num = next_page_num;
      cursor->cell_num = 0;
    } else {
      // 否则遍历到节点末尾，结束
      cursor->end_of_table = true;
    }
  }
}

InputBuffer *new_input_buffer() {
  InputBuffer *input_buffer = (InputBuffer *)malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;
  return input_buffer;
}

void print_prompt() { printf("db > "); }

void read_input(InputBuffer *input_buffer) {
  ssize_t bytes_read =
      getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }
  // ignore trailing newline
  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer *input_buffer) {
  free(input_buffer->buffer);
  free(input_buffer);
}

// 得到meta-commands的类型，'.exit'...
// do_meta_command 现在还是一个wrapper，为后面扩展留出空间
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table) {
  // meta-command .exit同时关闭数据库
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    // .exit命令同时关闭数据库，把内存中的page写入文件中
    db_close(table);
    exit(EXIT_SUCCESS);
  } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
    // 增加一个".btree" 元命令，打印出单表的叶子节点的信息
    printf("Tree:\n");
    print_tree(table->pager, 0, 0);
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
    // 增加一个".constant" 元命令
    printf("Constants:");
    print_constants();
    return META_COMMAND_SUCCESS;
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

// insert语句的字符解析
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement) {
  statement->type = STATEMENT_INSERT;

  // 注意strtok这个函数是有副作用的，多次调用表现不同；
  // 第一次调用的时候，str为要解析的字符串，delim为要分割的字符串
  // 继续分割同一个字符串的时候，str设置为NULL，delim可以设置为不同值
  // 插入语句eg：`insert {id} {username} {email}`，根据空格分割
  strtok(input_buffer->buffer, " ");
  char *id_string = strtok(NULL, " ");
  char *username = strtok(NULL, " ");
  char *email = strtok(NULL, " ");

  if (id_string == NULL || username == NULL || email == NULL) {
    return PREPARE_SYNTAX_ERROR;
  }
  int id = atoi(id_string);
  if (id < 0) {
    return PREPARE_NEGATIVE_ID;
  }
  if (strlen(username) > COLUMN_USERNAME_SIZE ||
      strlen(email) > COLUMN_EMAIL_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }

  statement->row_to_insert.id = id;
  strcpy(statement->row_to_insert.username, username);
  strcpy(statement->row_to_insert.email, email);
  return PREPARE_SUCCESS;
}

// 得到statement的类型，select、insert...
PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    return prepare_insert(input_buffer, statement);
  }
  // 只要select语句的前6个字符是`select`，那就执行select，因为只有一个表。
  // 也不考虑只select一部分字段
  if (strncmp(input_buffer->buffer, "select", 6) == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }
  return PREPARE_UNRECOGNIZED_STATEMENT;
}

void print_row(Row *row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

// 执行查找语句，把所有的row列都打印出来
ExecuteResult execute_select(Statement *statement, Table *table) {
  Row row;
  // 先把游标设置为文件开头
  Cursor *cursor = table_start(table);

  while (!(cursor->end_of_table)) {
    // 把游标位置的一行数据反序列化到row
    deserialize_row(cursor_value(cursor), &row);
    // 打印这一行数据
    print_row(&row);
    // 游标下移一位
    cursor_advance(cursor);
  }

  // 退出while，表示cursor到了数据库表的末尾，释放cursor；
  free(cursor);
  return EXECUTE_SUCCESS;
}

// 执行插入语句
ExecuteResult execute_insert(Statement *statement, Table *table) {
  // 因为现在还没实现完整的B+树，
  // B+树的根节点也就是叶子结点。所以这儿直接得到root_page，就是唯一的page
  // get_page 会把分配的这一页page放到自己的缓存之中，缓存的数据结构是指针数组
  void *node = get_page(table->pager, table->root_page_num);

  // 此节点的数据条数
  // 在part10中，如果节点满了，叶子结点分裂成两个，根节点变成内部节点。
  // 但是这儿的leaf_ndoe_num_cells仍然把这个内部节点当做叶子结点，
  // 所以说，此时还不能对内部节点插入数据。
  uint32_t num_cells = *leaf_node_num_cells(node);

  // 要插入的一条数据
  Row *row_to_insert = &(statement->row_to_insert);
  // 得到一个指向表末尾的cursor游标，现在还只是在数据库文件的末尾插入
  // Cursor *cursor = table_end(table);

  // 硬编码的数据库表(id, username, email)，其中id就是key
  uint32_t key_to_insert = row_to_insert->id;

  // 游标找到要插入数据的位置，这个是根据key的位置找的
  // 在数据库的表中，key是升序排列的，
  Cursor *cursor = table_find(table, key_to_insert);

  // 游标位置小于这个node的数据条数，那就需要判断一下key
  if (cursor->cell_num < num_cells) {
    // 得到游标位置的一条记录的key，
    uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
    // 已经存在相同key的一条记录
    if (key_at_index == key_to_insert) {
      return EXECUTE_DUPLICATE_KEY;
    }
  }

  // 然后把数据写到这一行，覆盖掉原来的数据
  leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
  // 释放掉cursor空间
  free(cursor);

  return EXECUTE_SUCCESS;
}

// 执行SQL语句；根据不同类型：select、insert、delete等做switch；
ExecuteResult execute_statement(Statement *statement, Table *table) {
  switch (statement->type) {
    case (STATEMENT_INSERT):
      return execute_insert(statement, table);
    case (STATEMENT_SELECT):
      return execute_select(statement, table);
  }
}

// 打开底层存储文件
Pager *pager_open(const char *filename) {
  // pager打开文件需要可读可写，文件不存在要创建，用户读写权限
  // FIXME：这儿的open打开权限有问题，后面需要修改
  int fd = open(filename, O_RDWR | O_CREAT, 0777);
  if (fd == -1) {
    printf("Unable to open file\n");
    exit(EXIT_FAILURE);
  }
  // 得到文件长度
  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager *pager = (Pager *)malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;
  pager->num_pages = (file_length / PAGE_SIZE);

  // 检查文件长度是否是PAGE_SIZE的倍数
  if (file_length % PAGE_SIZE != 0) {
    printf("DB file is not a whole number of pages. Corrupt file.\n");
    exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }
  return pager;
}

// 初始化数据库表
// Table *new_table() {
//   Table *table = (Table *)malloc(sizeof(Table));
//   table->num_rows = 0;
//   for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
//     table->pages[i] = NULL;
//   }
//   return table;
// }

// 打开数据库表，初始化pager，初始化table
Table *db_open(const char *filename) {
  Pager *pager = pager_open(filename);

  Table *table = malloc(sizeof(Table));
  table->pager = pager;
  table->root_page_num = 0;

  if (pager->num_pages == 0) {
    // new database file, initialize page 0 as leaf node.
    void *root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
    set_node_root(root_node, true);
  }
  return table;
}

// 表示内部节点包括的key数量，最大是510个
uint32_t *internal_node_num_keys(void *node) {
  return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}
// 这个指针指向的是内部节点的(最)右边的节点指针
uint32_t *internal_node_right_child(void *node) {
  return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}
// 返回内部节点中第cell_num个cell的地址
uint32_t *internal_node_cell(void *node, uint32_t cell_num) {
  return node + INTERNAL_NODE_HEADER_SIZE + INTERNAL_NODE_CELL_SIZE * cell_num;
}

// 返回internal_node中第child_num个子节点指针
uint32_t *internal_node_child(void *node, uint32_t child_num) {
  uint32_t num_keys = *internal_node_num_keys(node);
  // child_num大于这个internal_node中的子节点数量
  if (child_num > num_keys) {
    printf("Tried to access child_num %d > num_keys +%d\n", child_num,
           num_keys);
    exit(EXIT_FAILURE);
  } else if (child_num == num_keys) {
    // 要最后一个子节点，直接返回right_child
    return internal_node_right_child(node);
  } else {
    // 否则挨个找，找到第child_num个子节点
    return internal_node_cell(node, child_num);
  }
}

// 返回internal_node中第key_num个子节点对应的key值
uint32_t *internal_node_key(void *node, uint32_t key_num) {
  return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

// 返回内部节点中最大的key值
// 这个可以在判断key对应的数据存在不存在，大于这个值肯定不存在
uint32_t get_node_max_key(void *node) {
  switch (get_node_type(node)) {
    case NODE_INTERNAL:
      // 返回node节点中最后一个key，key是按照升序排列的，最后一个就是最大的
      return *internal_node_key(node, *internal_node_num_keys(node) - 1);
    case NODE_LEAF:
      // 叶子结点
      // 返回叶子结点中最大key值，这个是遍历顺序存储在node中的记录的key
      return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
  }
}

// 判断节点是不是根节点
bool is_node_root(void *node) {
  // 记录是否是根节点，用了uint8数据类型
  uint8_t value = *(uint8_t *)(node + NODE_TYPE_OFFSET);
  return (bool)value;
}
// 把这个节点设置为root节点
void set_node_root(void *node, bool is_root) {
  uint8_t value = (uint8_t)is_root;
  // 这儿的错误，太蠢了
  *(uint8_t *)(node + IS_ROOT_OFFSET) = value;
}

// B+树中节点的父指针
uint32_t *node_parent(void *node) { return node + PARENT_POINTER_OFFSET; }

// 修改内部节点中，指向某个子节点的key
void update_internal_node_key(void *node, uint32_t old_key, uint32_t new_key) {
  uint32_t old_child_index = internal_node_find_child(node, old_key);
  *internal_node_key(node, old_child_index) = new_key;
}

// 关键的函数，指定了B+树中的父子节点索引，根据子节点中的最大key值确定插入位置。
void internal_node_insert(Table *table, uint32_t parent_page_num,
                          uint32_t child_page_num) {
  void *parent = get_page(table->pager, parent_page_num);
  void *child = get_page(table->pager, child_page_num);
  uint32_t child_max_key = get_node_max_key(child);
  // 根据child节点的key，找到child节点的插入位置index
  uint32_t index = internal_node_find_child(parent, child_max_key);
  // 原来parent节点的子节点个数
  uint32_t original_num_keys = *internal_node_num_keys(parent);
  *internal_node_num_keys(parent) = original_num_keys + 1;

  // 内部节点的子节点树，已经装满了
  if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
    printf("Need to implement splitting internal node\n");
    exit(EXIT_FAILURE);
  }

  // 为啥B+树中的内部节点要单独设置一个right child ?，导致很多麻烦
  uint32_t right_child_page_num = *internal_node_right_child(parent);
  void *right_child = get_page(table->pager, right_child_page_num);
  // 如果要插入的这个叶子结点，比原来内部节点中做右侧的树的key还大
  if (child_max_key > get_node_max_key(right_child)) {
    // 要插入节点的最大key比原来的最右节点的key还大，县坝上最右节点放在左侧，然后把要插入的节点放在最右；
    *internal_node_child(parent, original_num_keys) = right_child_page_num;
    *internal_node_key(parent, original_num_keys) =
        get_node_max_key(right_child);
    *internal_node_right_child(parent) = child_page_num;
  } else {
    // 为新插入的子节点腾出位置
    // 挨个往后挪一个位置
    for (uint32_t i = original_num_keys; i > index; i--) {
      void *destination = internal_node_cell(parent, i);
      void *source = internal_node_cell(parent, i - 1);
      memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
    }
    // 最后空出来的位置，把要插入的子节点写入到这个位置
    *internal_node_child(parent, index) = child_page_num;
    *internal_node_key(parent, index) = child_max_key;
  }
}

// 叶子结点的分裂算法
void leaf_node_split_and_insert(Cursor *cursor, uint32_t key, Row *value) {
  // 1、分配一个新的叶子结点，把一般的数据拷贝到新的叶子节点中
  // 2、把当前的K-V记录,插入新旧两个叶子节点中的一个
  // 3、更新两个叶子节点的父子关系

  void *old_node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t old_max = get_node_max_key(old_node);
  // 直接返回pager的page数量，此时pager中这个page未分配，就会在get_page中分配新的页面
  uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
  void *new_node = get_page(cursor->table->pager, new_page_num);
  // 初始化叶子结点，
  initialize_leaf_node(new_node);
  *node_parent(new_node) = *node_parent(old_node);

  // 设置叶子结点的下一个指针，相当于链表插入，新节点在原来的节点之后
  *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
  // 并不是指针，而是节点号
  *leaf_node_next_leaf(old_node) = new_page_num;

  // 需要考虑的数据包括这个叶子结点的全部数据以及即将插入的这个数据
  // 要把这些数据分成两部分，其中的一部分拷贝到新的叶子节点上
  for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
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
      // serialize_row(value, destination);
      serialize_row(value,
                    leaf_node_value(destination_node, index_within_node));
      *leaf_node_key(destination_node, index_within_node) = key;
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
  // 到此位置，old_node、new_node没有建立联系
  if (is_node_root(old_node)) {
    return create_new_root(cursor->table, new_page_num);
  } else {
    // TODO：非根节点的情况暂不考虑
    // 在现在硬编码的情况下，一个数据库表最多对应的B+树的深度也就是2，所以不存在这种情况。
    // printf("Need to implement updating parent after split\n");
    // exit(EXIT_FAILURE);
    uint32_t parent_page_num = *node_parent(old_node);
    uint32_t new_max = get_node_max_key(old_node);
    void *parent = get_page(cursor->table->pager, parent_page_num);
    update_internal_node_key(parent, old_max, new_max);
    internal_node_insert(cursor->table, parent_page_num, new_page_num);
    return;
  }
}

// 为两个叶子结点创造一个root节点
// 调用此函数的时候，已经分配了root节点的右孩子节点，已经把上半部分数据拷贝到了右孩子节点了；
// 在此函数中，分配一个新的节点作为左孩子，然后拷贝数据，
// 这个的参数为什么这么奇怪？left_num就是cursor指向的位置，所以right_num只能以参数的方式给出？是吗？
void create_new_root(Table *table, uint32_t right_child_page_num) {
  //
  void *root = get_page(table->pager, table->root_page_num);
  void *right_child = get_page(table->pager, right_child_page_num);
  // 分配了左孩子节点，left child应该是新分配的节点
  uint32_t left_child_page_num = get_unused_page_num(table->pager);
  // 这个left_child是新分配的，
  void *left_child = get_page(table->pager, left_child_page_num);

  // 把root节点的数据拷贝到左孩子节点
  memcpy(left_child, root, PAGE_SIZE);

  // root节点的数据拷贝到left_child，然后取消掉root标记
  set_node_root(left_child, false);
  // BUG:FIXME,这儿应该还需要把left_child这个节点设置为叶子结点?
  set_node_type(left_child, NODE_LEAF);
  set_node_type(root, NODE_INTERNAL);

  // 这儿为什么要memcpy拷贝一次数据呢？为啥不直接把root当做left_child，然后把新分配空间的page当做root
  // 还能少memcpy一次？

  // 最后把root设置为internal内部节点，设置左右两个孩子
  initialize_internal_node(root);
  // 原来的root仍然是新的root
  set_node_root(root, true);
  // 设置内部节点的key为1，数量
  *internal_node_num_keys(root) = 1;
  // 并且指定内部节点的第一个孩子节点的page_num，主义者而不是第一个孩子节点的地址，而是page_num，解耦
  *internal_node_child(root, 0) = left_child_page_num;
  // 设置内部节点第1个key值是left_child中最大的key值。
  uint32_t left_child_max_key = get_node_max_key(left_child);
  *internal_node_key(root, 0) = left_child_max_key;
  // 同样的方法，设置root的右孩子节点，只需要设置page_num。
  *internal_node_right_child(root) = right_child_page_num;
  *node_parent(left_child) = table->root_page_num;
  *node_parent(right_child) = table->root_page_num;
}

// pager是数据库在内存中的缓存，用的数据结构是指针数组，
// 直接返数组中已经使用了的长度，
// TODO：这个可能后面需要改成循环队列，或者什么调度算法
uint32_t get_unused_page_num(Pager *pager) { return pager->num_pages; }

// 把k/v数据插入到叶子结点的cursor游标位置
void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value) {
  // 得到游标指向的node，
  void *node = get_page(cursor->table->pager, cursor->page_num);
  // 得到这个node上的数据条数
  uint32_t num_cells = *leaf_node_num_cells(node);

  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    // 这个节点装满了，需要分裂叶子结点，
    leaf_node_split_and_insert(cursor, key, value);
    return;
  }
  // 游标所在的数据位置小于整个node的数据条数
  // 就是说这条数据需要插在既有数据中间
  // 那就是说需要给留出位置
  if (cursor->cell_num < num_cells) {
    // 插入数据的时候，需要把后面的每条数据依次后移一位。为当前插入的数据腾出空间。
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
             LEAF_NODE_CELL_SIZE);
    }
  }
  // 这个node的数据条数加一
  *(leaf_node_num_cells(node)) += 1;
  *(leaf_node_key(node, cursor->cell_num)) = key;
  serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

// flush数据到数据库文件
// 因为使用B+树之后，每个节点node的大小就是page(4096B)
// 即使这个节点没有写满，也直接申请一个page大小的空间
// 因此在写磁盘的时候，就不用考虑不够一个page的部分了
void page_flush(Pager *pager, uint32_t page_num) {
  if (pager->pages[page_num] == NULL) {
    printf("Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
  if (offset == -1) {
    printf("Error seeking\n");
    exit(EXIT_FAILURE);
  }
  // 之后一部分不够一页的数据直接写满一页
  ssize_t bytes_written =
      write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
  if (bytes_written == -1) {
    printf("Erroring writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

// 关闭数据库的时候，需要把cache写会文件
void db_close(Table *table) {
  Pager *pager = table->pager;
  // 数据库占用的page在最后可能存在碎片，最后的碎片不需要把整个page大小的文件
  // uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

  // flush完整的page页
  for (uint32_t i = 0; i < pager->num_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    // 写文件
    page_flush(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }
  // 没有最后不完整的page页

  // 关闭文件
  int result = close(pager->file_descriptor);
  if (result == -1) {
    printf("Error closing db\n");
    exit(EXIT_FAILURE);
  }
  // 最后释放内存，包括pager->pages，以及pager，table
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void *page = pager->pages[i];
    if (page != NULL) {
      free(page);
      pager->pages[i] = NULL;
    }
  }
  free(pager);
  free(table);
}

//
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

// Cursor *table_start(Table *table) {
//   table_find()
//   Cursor *cursor = malloc(sizeof(Cursor));
//   cursor->table = table;
//   cursor->page_num = table->root_page_num;  // 初始化cursor的page_num
//   cursor->cell_num = 0;  // 设置cursor的cell为0，即指向node的第一条记录

//   //
//   void *root_node = get_page(table->pager, table->root_page_num);
//   uint32_t num_cells = *leaf_node_num_cells(root_node);
//   cursor->end_of_table = (num_cells == 0);

//   return cursor;
// }
// 游标cursor指向数据库表的末尾
// Cursor *table_end(Table *table) {
//   Cursor *cursor = malloc(sizeof(Cursor));
//   cursor->table = table;
//   cursor->page_num = table->root_page_num;

//   void *root_node = get_page(table->pager, table->root_page_num);
//   uint32_t num_cells = *leaf_node_num_cells(root_node);
//   cursor->cell_num = num_cells;

//   cursor->end_of_table = true;
//   return cursor;
// }

uint32_t internal_node_find_child(void *node, uint32_t key) {
  // 根据内部节点，以及子节点的key，返回子节点的索引
  uint32_t num_keys = *internal_node_num_keys(node);
  // 二分查找key，
  uint32_t min_index = 0;
  uint32_t max_index = num_keys;  // 比最大索引大1
  while (min_index != max_index) {
    uint32_t index = (min_index + max_index) / 2;
    uint32_t key_to_right = *internal_node_key(node, key);
    if (key_to_right >= key) {
      max_index = index;
    } else {
      min_index = index + 1;
    }
  }
  return min_index;
}

Cursor *internal_node_find(Table *table, uint32_t page_num, uint32_t key) {
  // 先得到这个数据库表的根节点
  void *node = get_page(table->pager, page_num);

  uint32_t child_index = internal_node_find_child(node, key);
  uint32_t child_num = *internal_node_child(node, child_index);
  void *child = get_page(table->pager, child_num);

  switch (get_node_type(child)) {
    case NODE_LEAF:
      return leaf_node_find(table, child_num, key);
    case NODE_INTERNAL:
      return internal_node_find(table, child_num, key);
  }
}

// 根据数据库表中的key设置游标
// 在数据库表中查key值，设置cursor指向此处
Cursor *table_find(Table *table, uint32_t key) {
  uint32_t root_page_num = table->root_page_num;
  // 数据库的node，一个页面
  void *root_page = get_page(table->pager, root_page_num);
  // 判断这个nod而必须是叶子结点，只有叶子结点存储完整数据
  if (get_node_type(root_page) == NODE_LEAF) {
    // 在这个node中根据key值查找记录，返回游标
    return leaf_node_find(table, root_page_num, key);
  } else {
    // 在内部节点中，根据key值得到数据库表中的cursor
    return internal_node_find(table, root_page_num, key);
  }
}

// 在一个node中，根据key设置游标
Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key) {
  // 得到这个node
  void *node = get_page(table->pager, page_num);
  // 当前node的cells数量
  uint32_t num_cells = *leaf_node_num_cells(node);

  Cursor *cursor = (Cursor *)malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = page_num;

  // binary search， 在node中二分查找key
  uint32_t min_index = 0;
  uint32_t one_past_max_index = num_cells;
  while (one_past_max_index != min_index) {
    uint32_t index = (one_past_max_index + min_index) / 2;
    uint32_t key_at_index = *leaf_node_key(node, index);
    if (key_at_index == key) {
      cursor->cell_num = index;
      return cursor;
    } else if (key_at_index > key) {
      one_past_max_index = index;
    } else {
      min_index = index + 1;
    }
  }
  //到这儿说明max_index == min_index，随便设置都可
  cursor->cell_num = min_index;
  return cursor;
}

int main(int argc, char *argv[]) {
  InputBuffer *input_buffer = new_input_buffer();
  // // 初始化单个数据库表
  // Table *table = new_table();

  // 通过argv提供一个参数，表示数据库表名称
  if (argc < 2) {
    printf("Must supply a database filename.\n");
    exit(EXIT_FAILURE);
  }

  char *filename = argv[1];
  Table *table = db_open(filename);

  while (true) {
    print_prompt();
    read_input(input_buffer);

    // meta-commands
    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer, table)) {
        case (META_COMMAND_SUCCESS):
          continue;
        case (META_COMMAND_UNRECOGNIZED_COMMAND):
          printf("Unrecognized command '%s'.\n", input_buffer->buffer);
          continue;
      }
    }
    // statement-commands
    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
      case (PREPARE_SUCCESS):
        break;
      case (PREPARE_NEGATIVE_ID):
        printf("ID must be positive.\n");
        continue;
      case (PREPARE_STRING_TOO_LONG):
        printf("String is too long.\n");
        continue;
      case (PREPARE_SYNTAX_ERROR):
        printf("syntax error. could not parse statement\n");
        continue;
      case (PREPARE_UNRECOGNIZED_STATEMENT):
        printf("Unrecognized keyword at start of '%s'.\n",
               input_buffer->buffer);
        continue;
    }

    switch (execute_statement(&statement, table)) {
      case (EXECUTE_SUCCESS):
        printf("Executed.\n");
        break;
      case (EXECUTE_DUPLICATE_KEY):
        printf("Error: Duplicate key.\n");
        break;
      case (EXECUTE_TABLE_FULL):
        printf("Error: Table full.\n");
        break;
    }
  }
}
