# pingcap-assignment

- 第一天
  - 脑子里想方案
  - 去县里治灰指甲
  - 新电脑装环境
  
- 第二天
  - 继续想方案
  - 开始写归并排序和败者树
  - 调试Bug
    - 改io为bufio后发现的边界问题
    - 代码改动不一致导致的坑
    - ....
  
- 第三天
  - 调试Bug
    - write()/buffer -> buffer应cap但Length为0
    - 调试败者树
    - ....
    - 记录是个好习惯，后面修bug最好修一个bug就简单记录一下，总是修一样的bug不太好，和时间做朋友不是仅仅每天做时间记录（培养时间敏感度），而是应该和事情本身也要做朋友
  - 写完了归并（包括败者树）
  - 写了bloomfilter

写的很快乐和兴奋，怕今天又睡不着觉，和昨天一样写到5点才睡

果然，今天又写到五点才睡，很开心，大三下-大四上这一年，我已经逐渐怀疑人生，刷面经，刷算法题，实习搬砖，怀疑我是否真的喜欢写代码了，这两天写的兴奋到停不下来，看来还是挺喜欢写代码的，找回了以前难得的感觉，不仅是学的快乐，写的也快乐

虽然还是很菜，但是对自己后面撸一个简单的db还是有信心一点了
  
- 第4天
  - 不想记录了是怎么回事？哦，原来是我懒
  - 写完了buffer部分
  - 面向流程写代码/(ㄒoㄒ)/~~，开着debugger，debugger到哪里写哪里
  

  
## 题目

某个机器的配置为：CPU 8 cores, MEM 4G, HDD 4T
这个机器上有一个 1T 的无序数据文件，格式为 (key_size, key, value_size, value)，所有 key 都不相同。

设计一个索引结构，使得并发随机地读取每一个 key-value 的代价最小；读取时 key 必然在文件中且大致符合齐夫分布。
允许对数据文件做任意预处理，但是预处理的时间计入到整个读取过程的代价里