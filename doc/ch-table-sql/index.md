# Table API & SQL

```{tableofcontents}
```

在之前的章节中，我们已经系统介绍了如何使用 Flink 的 DataStream API 在时间维度上进行有状态的计算。为了方便开发和迭代，Flink 在 DataStream/DataSet API 之上提供了一个更高层的关系型数据库式的 API——Table API & SQL。Table API & SQL 有以下特点：

* 结合了流处理和批处理两种场景，提供统一的对外接口。
* Table API & SQL 均以关系型数据库中的表为基础模型，Table API 和 SQL 两者结合非常紧密。
* Table API & SQL 与其他平台使用习惯相似，例如 Hive SQL、Spark DataFrame & SQL、Python pandas 等，数据科学家可以快速从其他平台迁移到 Flink 平台上。
* 比起 DataStream/DataSet API，Table API & SQL 的开发成本较低，可以广泛应用在数据探索、业务报表、商业智能等各类场景，适合企业大规模推广。
* 很多用户对 Flink DataStream/DataSet API 的熟悉程度并不高，反而 Table API & SQL 在效率方面有很大优势：用户可以更关注业务逻辑，执行优化可以交由 Flink 来做。

基于 Table API & SQL 的诸多优点，Flink 社区非常重视对这方面的投入，无论是已经完成的版本还是中长期的规划中，Flink 社区都将 Table API & SQL 作为重要的发展方向。尤其是在在阿里巴巴在 Flink 社区投入更多的资源之后，阿里巴巴内部版本 Blink 和开源社区版本 Flink 正在快速融合，一些 Blink 中关于 Table API & SQL 的功能已经提交到开源社区版本中，Table API & SQL 处于快速迭代开发状态中。从另一方面来讲，Table API & SQL 的一些功能也在逐渐完善，一些接口也会发生变化。

由于批处理上的关系型查询已经比较成熟，相关书籍和材料已经比较丰富，因此这里不再花费精力详细介绍，本书主要围绕流处理场景来介绍 Table API & SQL。具体而言，我们将先概括性地介绍 Table API & SQL 的骨架程序和使用方法；接着重点介绍流处理下特有的概念：动态表和持续查询、时间和窗口、Join；然后介绍一些 Flink SQL 使用过程所涉及的一些重要知识点；最后介绍如何使用系统函数和用户自定义函数。