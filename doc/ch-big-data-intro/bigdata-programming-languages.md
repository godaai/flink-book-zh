(bigdata-programming-languages)=
# 编程语言的选择

大数据编程一般会使用 Java、Scala 和 Python 等编程语言，Flink 目前也支持上述 3 种语言，本节从大数据编程的角度来分析几种编程语言的优劣。

## Java 和 Scala

Java 是“老牌”企业级编程语言。Java 相比 C/C++ 更易上手，支持多线程，其生态圈中可用的第三方库众多。Java 虚拟机（Java Virtual Machine，JVM）保证了程序的可移植性，可以快速部署到不同计算机上，是很多分布式系统首选的编程语言，比如 Hadoop 和 Flink 的绝大多数代码都是用 Java 编写的，这些框架提供了丰富的文档，网络社区的支持好。因此，进行大数据编程，Java 编程是必备的技能。相比一些新型编程语言，Java 的缺点是代码有点冗长。

Scala 是一门基于 JVM 的编程语言。相比 Java，Scala 的特色是函数式编程。函数式编程非常适合大数据处理，我们将在第 2 章进一步介绍函数式编程思想。在并行计算方面，Scala 支持 Actor 模型，Actor 模型是一种更为先进的并行计算编程模型，很多大数据框架都基于 Actor 模型。Spark、Flink 和 Kafka 都是基于 Actor 模型的大数据框架。Scala 可以直接调用 Java 的代码，相比 Java，Scala 代码更为简洁和紧凑。凡事都有两面性，Scala 虽然灵活简洁，但是不容易掌握，即使是有一定 Java 基础的开发者，也需要花一定时间系统了解 Scala。

另外，Java 和 Scala 在互相学习和借鉴。Java 8 开始引入了 Lambda 表达式和链式调用，能够支持函数式编程，部分语法与 Scala 越来越接近，代码也更加简洁。

:::{note}

这里的 Lambda 表达式与 1.4.1 小节介绍的 Lambda 架构不同。Lambda 表达式基于函数式编程，是一种编写代码的方式。Lambda 架构主要指如何同时处理流批数据，是一种大数据架构。

:::

Flink 的核心代码由 Java 和 Scala 编写，为这两种语言提供丰富强大的 API，程序员可根据自己和团队的习惯从 Java 和 Scala 中选择。本书基于以下两点考虑，决定主要以 Java 来演示 Flink 的编程。

- Flink 目前绝大多数代码和功能均由 Java 实现，考虑到本书会展示一些 Flink 中基于 Java 的源码和接口，为了避免读者在 Java 和 Scala 两种语言间混淆，将主要使用 Java 展示一些 Flink 的核心概念。
- 不同读者的编程语言基础不一样，Scala 用户往往有一定的 Java 编程基础，而 Java 用户可能对 Scala 并不熟悉。而且 Scala 的语法非常灵活，一不小心可能出现莫名其妙的错误，初学者难以自行解决，而 Scala 相对应的书籍和教程不多。或者说 Scala 用户一般能够兼容 Java，而 Java 用户学习 Scala 的成本较高。

此外，由于大多数 Spark 作业基于 Scala，很多大数据工程师要同时负责 Spark 和 Flink 两套业务逻辑，加上 Flink 的 Scala API 与 Spark 比较接近，本书也会在一些地方提示 Scala 用户在使用 Flink 时的必要注意事项，并在随书附赠的工程中提供 Java 和 Scala 两个版本的代码，方便读者学习。

## Python

Python 无疑是近几年来编程语言界的“明星”。Python 简单易用，有大量第三方库，支持 Web、科学计算和机器学习，被广泛应用到人工智能领域。大数据生态圈的各项技术对 Python 支持力度也很大，Hadoop、Spark、Kafka、HBase 等技术都有 Python 版本的 API。鉴于 Python 在机器学习和大数据领域的流行程度，Flink 社区非常重视对 Python API 的支持，正在积极完善 Flink 的 Python 接口。相比 Java 和 Scala，Python API 还处于完善阶段，迭代速度非常快。Flink 的 Python API 名为 PyFlink，是在 1.9 版本之后逐渐完善的，但相比 Java 和 Scala 还不够完善。考虑到 Python 和 Java/Scala 有较大区别，本书的绝大多数内容均基于 Java 相关知识，且 PyFlink 也在不断迭代、完善，本书暂时不探讨 PyFlink。

## SQL

严格来说，SQL 并不是一种全能的编程语言，而是一种在数据库上对数据进行操作的语言，相比 Java、Scala 和 Python，SQL 的上手门槛更低，它在结构化数据的查询上有绝对的优势。一些非计算机相关专业出身的读者可以在短期内掌握 SQL，并进行数据分析。随着数据科学的兴起，越来越多的岗位开始要求候选人有 SQL 技能，包括数据分析师、数据产品经理和数据运营等岗位。Flink 把这种面向结构化查询的需求封装成了表（Table），对外提供 Table API 和 SQL 的调用接口，提供了非常成熟的 SQL 支持。SQL 的学习和编写成本很低，利用它能够处理相对简单的业务逻辑，其非常适合在企业内被大规模推广。本书第 8 章将重点介绍 Table API 和 SQL 的使用方法。
