### lab1:使用Distributed的Map/Reduce方法实现统计单词个数及列举单词的反向索引
**test**
* 分别检测使用Sequential和Distributed模式统计的实验结果是否正确
* 使用Distributed模式时，一些worker（线程）在未完成统计却自动退出的情况下是否能正确执行
### lab2:raft协议的模拟实现
**test**
* leader和term在没有发生故障时是否仍保持不变
* leader挂掉后重新选主，不足半数以上不能选主
* 对于commit的客户端申请是否能重新同步
* 选主时是否参照了日志的index
* 是否能顺序的执行多个客户端并发发送的多条请求
* 客户端传送给孤立的leader的cmd不重传将会丢失
* 检测每个实例的persist
* leader每次在同步完所有日之后才提交commit而不是逐一提交
* 检测在网络传输不稳定的情况下是否能正确传输
### lab3:raft协议的snapshot实现及在之前实现的raft协议上模拟一个简单的kvRaft结构
**test**
* 日志长度是否会超过指定大小
* 对kv数据库的基本Put(),Get(),Append()操作
* 发生部分服务器失联，重启后是否可以通过snapshot恢复正常工作
* 检测存储的snapshot文件长度
### lab4:shardKv的简单模拟实现
**test**
* shardMaster是否能完成基本的Join, Move, Leave操作，并能适应并发的请求
* shard所分配的gid是否能保证平均
* config是否能够正确的变动
* 每次移动是否能保证变动最少的shard
* shardKv在关闭重启group后能否正常完成对数据库的Put, Get, Append操作
* 执行Join, Leave操作后，shardKv的config是否正确变动
* 重启server检测snapshot功能及snapshot存储文件大小
* 在网络环境不稳定的情况下能否正常工作
* gc功能
* 在由于一些服务器挂掉后,只完成了部分Mirgate和gc操作的情况下，客户端能否访问部分完成shard所包含的数据库内容
---
