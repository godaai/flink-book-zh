# Table API & SQL

```{tableofcontents}
```

在之前的章节中，我们已经系统介绍了如何使用Flink的DataStream API在时间维度上进行有状态的计算。为了方便开发和迭代，Flink在DataStream/DataSet API之上提供了一个更高层的关系型数据库式的API——Table API & SQL。Table API & SQL有以下特点：

* 结合了流处理和批处理两种场景，提供统一的对外接口。
* Table API & SQL均以关系型数据库中的表为基础模型，Table API和SQL两者结合非常紧密。
* Table API & SQL与其他平台使用习惯相似，例如Hive SQL、Spark DataFrame & SQL、Python pandas等，数据科学家可以快速从其他平台迁移到Flink平台上。
* 比起DataStream/DataSet API，Table API & SQL的开发成本较低，可以广泛应用在数据探索、业务报表、商业智能等各类场景，适合企业大规模推广。
* 很多用户对Flink DataStream/DataSet API的熟悉程度并不高，反而Table API & SQL在效率方面有很大优势：用户可以更关注业务逻辑，执行优化可以交由Flink来做。

基于Table API & SQL的诸多优点，Flink社区非常重视对这方面的投入，无论是已经完成的版本还是中长期的规划中，Flink社区都将Table API & SQL作为重要的发展方向。尤其是在在阿里巴巴在Flink社区投入更多的资源之后，阿里巴巴内部版本Blink和开源社区版本Flink正在快速融合，一些Blink中关于Table API & SQL的功能已经提交到开源社区版本中，Table API & SQL处于快速迭代开发状态中。从另一方面来讲，Table API & SQL的一些功能也在逐渐完善，一些接口也会发生变化。

由于批处理上的关系型查询已经比较成熟，相关书籍和材料已经比较丰富，因此这里不再花费精力详细介绍，本书主要围绕流处理场景来介绍Table API & SQL。具体而言，我们将先概括性地介绍Table API & SQL的骨架程序和使用方法；接着重点介绍流处理下特有的概念：动态表和持续查询、时间和窗口、Join；然后介绍一些Flink SQL使用过程所涉及的一些重要知识点；最后介绍如何使用系统函数和用户自定义函数。