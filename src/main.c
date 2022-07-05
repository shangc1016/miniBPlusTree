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

typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL } ExecuteResult;

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

// 这个就是数据库表的page指针数组，包括了表的行数；
typedef struct {
  uint32_t num_rows;
  Pager *pager;
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
// 分辨内部节点以及叶子节点
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
const uint32_t LEAF_NODE_HEADER_SIZE =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

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

// function declaration =============================
// 序列化、反序列化
void serialize_row(Row *, void *);
void deserialize_row(void *, Row *);

void db_close(Table *);
Table *db_open(const char *);

void *get_page(Pager *, uint32_t);
void *row_slot(Table *, uint32_t);
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
void initalize_leaf_node(void *);
// function declaration end =========================

// part8，访问key、value以及元数据都需要用到上面定义好的宏，以及指针算术运算
// 返回这个node节点的cell数据条数，根据指针运算，node地址加上CELLS的偏移
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
void initalize_leaf_node(void *node) { *leaf_node_num_cells(node) = 0; }
// part8 end

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
    printf("Tried tyo fetch page number out of bounds. %d -> %d\n", page_num,
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
      // 从文件中读到了page中，更新缓存
      pager->pages[page_num] = page;

      // TODO，这是啥意思？
      if (page_num >= pager->num_pages) {
        pager->num_pages = page_num + 1;
      }

    } else {
      printf("Error page number\n");
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
  void *node = get_node(cursor->table->pager, page_num);

  cursor->cell_num +=1;
  if(cursor->cell_num >= (*leaf_node_num_cells(node))){
    cursor->end_of_table = true;
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
  char *keyword = strtok(input_buffer->buffer, " ");
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
  // 数据库表满了
  if (table->num_rows >= TABLE_MAX_ROWS) {
    return EXECUTE_TABLE_FULL;
  }
  // 要插入的一条数据
  Row *row_to_insert = &(statement->row_to_insert);
  // 得到一个指向表末尾的cursor游标
  Cursor *cursor = table_end(table);

  // 先找到数据库表，现在的行数
  uint32_t num_rows = table->num_rows;
  // 然后把数据写到这一行
  // serialize_row(row_to_insert, row_slot(table, num_rows));
  serialize_row(row_to_insert, cursor_value(cursor));
  // 释放掉cursor空间
  free(cursor);

  table->num_rows += 1;
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
  uint32_t num_rows = pager->file_length / ROW_SIZE;
  Table *table = malloc(sizeof(Table));
  table->pager = pager;
  table->num_rows = num_rows;
  return table;
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
  uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

  // flush完整的page页
  for (uint32_t i = 0; i < num_full_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    // 写文件
    page_flush(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }
  // 最后flush不完整的page页
  // uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
  // if (num_additional_rows > 0) {
  //   uint32_t page_num = num_full_pages;  // 完整的页面数量
  //   if (pager->pages[page_num] != NULL) {
  //     page_flush(pager, page_num, num_additional_rows * ROW_SIZE);
  //     free(pager->pages[page_num]);
  //     pager->pages[page_num] = NULL;
  //   }
  // }
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
  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = table->root_page_num;  // 初始化cursor的page_num
  cursor->cell_num = 0;  // 设置cursor的cell为0，即指向node的第一条记录

  //
  void *root_node = get_page(table->pager, table->root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(root_node);
  cursor->end_of_table = (num_cells == 0);

  return cursor;
}
// 游标cursor指向数据库表的末尾
Cursor *table_end(Table *table) {
  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = table->root_page_num;

  void *root_node = get_node(table->pager, table->root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(root_node);
  cursor->cell_num = num_cells;

  cursor->end_of_table = true;
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
      case (EXECUTE_TABLE_FULL):
        printf("Error: Table full.\n");
        break;
    }
  }
}
