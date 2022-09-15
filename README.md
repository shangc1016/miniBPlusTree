### sqlet


1、首先安装rspec
```shell
root@Legion:/home/sc/workspace/clang/sqlet# rspec --version
RSpec 3.11
  - rspec-core 3.11.0
  - rspec-expectations 3.11.1
  - rspec-mocks 3.11.1
  - rspec-support 3.11.1
```


在part4中，添加了rspec的测试
1. 在part4中，实现了内存kv数据库；所以测试了插入数据，查询数据；
2. 第二个测试：数据库表满了之后，继续插入数据打印table full
3. 硬编码的kv的value是(id, username, email)，测试一下各自最大的长度
4. 在插入数据超过最大长度的时候，打印错误信息
5. 插入数据的id不能为负数，否则打印错误信息
