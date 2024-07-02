(exercise-building-data-stream-using-Kafka)=
# 案例实战：使用Kafka构建文本数据流

尽管本书主题是Flink，但是对数据流的整个生命周期有一个更全面的认识有助于我们理解大数据和流处理。1.3.3小节简单介绍了Kafka这项技术，本节将介绍如何使用Kafka构建实时文本数据流，读者可以通过本节了解数据流管道的大致结构：数据生产者源源不断地生成数据流，数据流通过消息队列投递，数据消费者异步地对数据流进行处理。

## 1.7.1 Kafka和消息队列相关背景知识

### 1. 消息队列的功能

消息队列一般使用图1-6所示的“生产者-消费者”模型来解决问题：生产者生成数据，将数据发送到一个缓存区域，消费者从缓存区域中消费数据。消息队列可以解决以下问题：

- **系统解耦**：很多企业内部有众多系统，一个App也包含众多模块，如果将所有的系统和模块都放在一起作为一个庞大的系统来开发，未来则会很难维护和扩展。如果将各个模块独立出来，模块之间通过消息队列来通信，未来可以轻松扩展每个独立模块。另外，假设没有消息队列，M个生产者和N个消费者通信，会产生M×N个数据管道，消息队列将这个复杂度降到了M+N。
- **异步处理**：同步是指如果模块A向模块B发送消息，必须等待返回结果后才能执行接下来的业务逻辑。异步是消息发送方模块A无须等待返回结果即可继续执行，只需要向消息队列中发送消息，至于谁去处理这些消息、消息等待多长时间才能被处理等一系列问题，都由消费者负责。异步处理更像是发布通知，发送方不用关心谁去接收通知、如何对通知做出响应等问题。
- **流量削峰**：电商促销、抢票等场景会对系统造成巨大的压力，瞬时请求暴涨，消息队列的缓存就像一个蓄水池，以很低的成本将上游的洪峰缓存起来，下游的数据处理模块按照自身处理能力从缓存中拉取数据，避免数据处理模块崩溃。
- **数据冗余**：很多情况下，下游的数据处理模块可能发生故障，消息队列将数据缓存起来，直到数据被处理，一定程度上避免了数据丢失风险。

Kafka作为一个消息队列，主要提供如下3种核心能力：

- 为数据的生产者提供发布功能，为数据的消费者提供订阅功能，即传统的消息队列的能力。
- 将数据流缓存在缓存区域，为数据提供容错性，有一定的数据存储能力。
- 提供了一些轻量级流处理能力。

可见Kafka不仅是一个消息队列，也有数据存储和流处理的功能，确切地说，Kafka是一个流处理系统。

### 2. Kafka的一些核心概念

Kafka涉及不少概念，包括Topic、Producer、Consumer等，这里从Flink流处理的角度出发，只对与流处理关系密切的核心概念做简单介绍。

- **Topic**：Kafka按照Topic来区分不同的数据。以淘宝这样的电商平台为例，某个Topic发布买家用户在电商平台的行为日志，比如搜索、点击、聊天、购买等行为；另外一个Topic发布卖家用户在电商平台上的行为日志，比如上新、发货、退货等行为。
- **Producer**：多个Producer将某份数据发布到某个Topic下。比如电商平台的多台线上服务器将买家行为日志发送到名为user_behavior的Topic下。
- **Consumer**：多个Consumer被分为一组，名为Consumer Group，一组Consumer Group订阅一个Topic下的数据。通常我们可以使用Flink编写的程序作为Kafka的Consumer来对一个数据流做处理。

## 1.7.2 使用Kafka构建一个文本数据流

### 1. 下载和安装

如前文所述，绝大多数的大数据框架基于Java，因此在进行开发之前要先搭建Java编程环境，主要是下载和配置Java开发工具包（Java Development Kit，JDK）。网络上针对不同操作系统的相关教程已经很多，这里不赘述。

从Kafka官网下载二进制文件形式的软件包，软件包扩展名为 .tgz。Windows用户可以使用7Zip或WinRAR软件解压 .tgz文件，Linux和macOS用户需要使用命令行工具，进入该下载目录。

```bash
$ tar -xzf kafka_2.12-2.3.0.tgz
$ cd kafka_2.12-2.3.0
```

**注意**：

`$`符号表示该行命令在类UNIX操作系统（macOS和Linux）命令行中执行，而不是在Python交互命令界面或其他任何交互界面中。Windows的命令行提示符是大于号`>`。

解压之后的文件中，`bin`目录默认为Linux和macOS设计。Windows用户要进入`bin\windows\`来启动相应脚本，且脚本文件扩展名要改为`.bat`。

### 2. 启动服务

Kafka使用ZooKeeper来管理集群，因此需要先启动ZooKeeper。刚刚下载的Kafka包里已经包含了ZooKeeper的启动脚本，可以使用这个脚本快速启动一个ZooKeeper服务。

```bash
$ bin/zookeeper-server-start.sh config/zookeeper.properties
```

启动成功后，对应日志将被输出到屏幕上。

接下来再开启一个命令行会话，启动Kafka：

```bash
$ bin/kafka-server-start.sh config/server.properties
```

以上两个操作均使用`config`文件夹下的默认配置文件，需要注意配置文件的路径是否写错。生产环境中的配置文件比默认配置文件复杂得多。

### 3. 创建Topic

开启一个命令行会话，创建一个名为`Shakespeare`的Topic：

```bash
$ bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Shakespeare
```

也可以使用命令查看已有的Topic：

```bash
$ bin/kafka-topics.sh --list --bootstrap-server localhost:9092
Shakespeare
```

### 4. 发送消息

接下来我们模拟 Producer，假设这个 Producer 是莎士比亚（Shakespeare）本人，它不断向“Shakespeare”这个Topic发送自己的最新作品：

```bash
$ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic Shakespeare
>To be, or not to be, that is the question:
```

每一行作为一条消息事件，被发送到了Kafka集群上，虽然这个集群只有本机这一台服务器。

### 5. 消费数据

另外一些人想了解莎士比亚向Kafka发送过哪些新作，所以需要使用一个Consumer来消费刚刚发送的数据。我们开启一个命令行会话来模拟Consumer：

```bash
$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic Shakespeare --from-beginning
To be, or not to be, that is the question:
```

Producer端和Consumer端在不同的命令行会话中，我们可以在Producer端的命令行会话里不断输入一些文本。切换到Consumer端后，可以看到相应的文本被发送了过来。

至此，我们模拟了一个实时数据流数据管道：不同人可以创建Topic，发布属于自己的内容；其他人可以订阅一个或多个Topic，根据需求设计后续处理逻辑。

使用Flink做流处理时，我们很可能以消息队列作为输入数据源，进行一定处理后，再输出到消息队列、数据库或其他组件上。