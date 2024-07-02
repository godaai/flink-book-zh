(bigdata-programming-languages)=
# 编程语言的选择

大数据编程一般会使用Java、Scala和Python等编程语言，Flink目前也支持上述3种语言，本节从大数据编程的角度来分析几种编程语言的优劣。

## 1.6.1 Java和Scala

Java是“老牌”企业级编程语言。Java相比C/C++更易上手，支持多线程，其生态圈中可用的第三方库众多。Java虚拟机（Java Virtual Machine，JVM）保证了程序的可移植性，可以快速部署到不同计算机上，是很多分布式系统首选的编程语言，比如Hadoop和Flink的绝大多数代码都是用Java编写的，这些框架提供了丰富的文档，网络社区的支持好。因此，进行大数据编程，Java编程是必备的技能。相比一些新型编程语言，Java的缺点是代码有点冗长。

Scala是一门基于JVM的编程语言。相比Java，Scala的特色是函数式编程。函数式编程非常适合大数据处理，我们将在第2章进一步介绍函数式编程思想。在并行计算方面，Scala支持Actor模型，Actor模型是一种更为先进的并行计算编程模型，很多大数据框架都基于Actor模型。Spark、Flink和Kafka都是基于Actor模型的大数据框架。Scala可以直接调用Java的代码，相比Java，Scala代码更为简洁和紧凑。凡事都有两面性，Scala虽然灵活简洁，但是不容易掌握，即使是有一定Java基础的开发者，也需要花一定时间系统了解Scala。

另外，Java和Scala在互相学习和借鉴。Java 8开始引入了Lambda表达式和链式调用，能够支持函数式编程，部分语法与Scala越来越接近，代码也更加简洁。

**注意**

这里的Lambda表达式与1.4.1小节介绍的Lambda架构不同。Lambda表达式基于函数式编程，是一种编写代码的方式。Lambda架构主要指如何同时处理流批数据，是一种大数据架构。

Flink的核心代码由Java和Scala编写，为这两种语言提供丰富强大的API，程序员可根据自己和团队的习惯从Java和Scala中选择。本书基于以下两点考虑，决定主要以Java来演示Flink的编程。

- Flink目前绝大多数代码和功能均由Java实现，考虑到本书会展示一些Flink中基于Java的源码和接口，为了避免读者在Java和Scala两种语言间混淆，将主要使用Java展示一些Flink的核心概念。
- 不同读者的编程语言基础不一样，Scala用户往往有一定的Java编程基础，而Java用户可能对Scala并不熟悉。而且Scala的语法非常灵活，一不小心可能出现莫名其妙的错误，初学者难以自行解决，而Scala相对应的书籍和教程不多。或者说Scala用户一般能够兼容Java，而Java用户学习Scala的成本较高。

此外，由于大多数Spark作业基于Scala，很多大数据工程师要同时负责Spark和Flink两套业务逻辑，加上Flink的Scala API与Spark比较接近，本书也会在一些地方提示Scala用户在使用Flink时的必要注意事项，并在随书附赠的工程中提供Java和Scala两个版本的代码，方便读者学习。

## 1.6.2 Python

Python无疑是近几年来编程语言界的“明星”。Python简单易用，有大量第三方库，支持Web、科学计算和机器学习，被广泛应用到人工智能领域。大数据生态圈的各项技术对Python支持力度也很大，Hadoop、Spark、Kafka、HBase等技术都有Python版本的API。鉴于Python在机器学习和大数据领域的流行程度，Flink社区非常重视对Python API的支持，正在积极完善Flink的Python接口。相比Java和Scala，Python API还处于完善阶段，迭代速度非常快。Flink的Python API名为PyFlink，是在1.9版本之后逐渐完善的，但相比Java和Scala还不够完善。考虑到Python和Java/Scala有较大区别，本书的绝大多数内容均基于Java相关知识，且PyFlink也在不断迭代、完善，本书暂时不探讨PyFlink。

## 1.6.3 SQL

严格来说，SQL并不是一种全能的编程语言，而是一种在数据库上对数据进行操作的语言，相比Java、Scala和Python，SQL的上手门槛更低，它在结构化数据的查询上有绝对的优势。一些非计算机相关专业出身的读者可以在短期内掌握SQL，并进行数据分析。随着数据科学的兴起，越来越多的岗位开始要求候选人有SQL技能，包括数据分析师、数据产品经理和数据运营等岗位。Flink把这种面向结构化查询的需求封装成了表（Table），对外提供Table API 和 SQL的调用接口，提供了非常成熟的SQL支持。SQL的学习和编写成本很低，利用它能够处理相对简单的业务逻辑，其非常适合在企业内被大规模推广。本书第8章将重点介绍Table API 和SQL的使用方法。