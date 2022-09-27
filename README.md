### sqlet



part13


在上一小节中，实现了select查询的时候，遍历叶子结点。但是这个遍历只能是遍历两个叶子结点，在叶子结点分裂上有个小bug。
新的叶子结点全部插入到了内部节点中第二个子节点的位置，导致系统中只能有两个叶子结点。
> 导致了一个问题，内部节点不支持插入多个叶子结点。


spec测试，插入20条数据，然后select数据

![20220923103023](https://note-img-1300721153.cos.ap-nanjing.myqcloud.com/md-img20220923103023.png)

> 可以看到，select查询到的数据没有问题；


spec测试，插入21条数据，然后select查询数据

![20220923103623](https://note-img-1300721153.cos.ap-nanjing.myqcloud.com/md-img20220923103623.png)

> TODO：现在有一个bug，插入数据超过两个叶子结点的时候，select查询数据会出错。



>因为之前已经设置了也个页面的大小4KB，一个数据库表最多使用100个页面。所以表中国年的记录条数是确定的。一个叶子结点最多插入13条记录，100个叶子结点最多有1300条记录。而内部节点中可以保存510个子节点。也就是说系统中功能的B+树的最大深度也就是2。




在part12小节中解决一个问题，就是叶子结点分裂之后，在原来的叶子结点中插入数据，导致原来的叶子结点再次分裂的问题。
> old叶子结点再次分裂，会占用新的叶子结点在内部节点中的位置，所以需要内部节点先给这个新的叶子结点腾出位置。但是在之前的小节中并没有考虑这个问题。



