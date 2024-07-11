(sec-flink-with-hadoop)=
# 与 Hadoop 集成

Flink 可以和 Hadoop 生态圈的组件紧密结合，比如 9.1 节中提到，Flink 可以使用 YARN 作为资源调度器，或者读取 HDFS、HBase 中的数据。在使用 Hadoop 前，我们需要确认已经安装了 Hadoop，并配置了环境变量 `HADOOP_CONF_DIR`，如下环境变量配置是 Hadoop 安装过程所必需的。

```bash
HADOOP_CONF_DIR=/path/to/etc/hadoop
```

此外，Flink 与 Hadoop 集成时，需要将 Hadoop 的依赖包添加到 Flink 中，或者说让 Flink 能够获取到 Hadoop 类。比如，使用 `bin/yarn-session.sh` 启动一个 Flink YARN Session 时，如果没有设置 Hadoop 依赖，将会出现下面的报错。

```java
java.lang.ClassNotFoundException: org.apache.hadoop.yarn.exceptions.YarnException
```

这是因为 Flink 源码中引用了 Hadoop YARN 的代码，但是在 Flink 官网提供的 Flink 下载包中，新版本的 Flink 已经不提供 Hadoop 集成，或者说，Hadoop 相关依赖包不会放入 Flink 包中。Flink 将 Hadoop 剔除的主要原因是 Hadoop 发布和构建的时间过长，不利于 Flink 的迭代。Flink 鼓励用户自己根据需要引入 Hadoop 依赖包，具体有如下两种方式。

1. 在环境变量中添加 Hadoop Classpath，Flink 从 Hadoop Classpath 中读取所需依赖包。
2. 将所需的 Hadoop 依赖包添加到 Flink 主目录下的 lib 目录中。

## 添加 Hadoop Classpath

Flink 使用环境变量 `$HADOOP_CLASSPATH` 来存储 Hadoop 相关依赖包的路径，或者说，`$HADOOP_CLASSPATH` 中的路径会添加到 `-classpath` 参数中。很多 Hadoop 发行版以及一些云环境默认情况下并不会设置这个变量，因此，执行 Hadoop 的各节点应该在其环境变量中设置 `$HADOOP_CLASSPATH`。

```bash
export HADOOP_CLASSPATH=`hadoop classpath`
```

上面的命令中，`hadoop` 是 Hadoop 提供的二进制命令工具，使用前必须保证 `hadoop` 命令添加到了环境变量 `$PATH` 中，`classpath` 是 `hadoop` 命令的一个参数选项。`hadoop classpath` 可以返回 Hadoop 所有相关的依赖包，将这些路径输出。如果在一台安装了 Hadoop 的节点上执行 `hadoop classpath`，下面是部分返回结果。

```plaintext
/path/to/hadoop/etc/hadoop:/path/to/hadoop/share/hadoop/common/lib/*:/path/to/hadoop/share/hadoop/yarn/lib/*:...
```

Flink 启动时，会从 `$HADOOP_CLASSPATH` 中寻找所需依赖包。这些依赖包来自节点所安装的 Hadoop，也就是说 Flink 可以和已经安装的 Hadoop 紧密结合起来。但 Hadoop 的依赖错综复杂，Flink 所需要的依赖和 Hadoop 提供的依赖有可能发生冲突。
该方式只需要设置 `$HADOOP_CLASSPATH`，简单快捷，缺点是有依赖冲突的风险。

## 将 Hadoop 依赖包添加到 lib 目录中

Flink 主目录下有一个 `lib` 目录，专门存放各类第三方的依赖包。Flink 程序启动时，会将 `lib` 目录加载到 Classpath 中。我们可以将所需的 Hadoop 依赖包添加到 `lib` 目录中。具体有两种获取 Hadoop 依赖包的方式：一种是从 Flink 官网下载预打包的 Hadoop 依赖包，一种是从源码编译。

Flink 社区帮忙编译生成了常用 Hadoop 版本的 Flink 依赖包，比如 Hadoop 2.8.3、Hadoop 2.7.5 等，使用这些 Hadoop 版本的用户可以直接下载这些依赖包，并放置到 `lib` 目录中。例如，Hadoop 2.8.3 的用户可以下载 `flink-shaded-Hadoop-2-uber-2.8.3-10.0.jar`，将这个依赖包添加到 Flink 主目录下的 `lib` 目录中。

如果用户使用的 Hadoop 版本比较特殊，不在下载列表里，比如是 Cloudera 等厂商发行的 Hadoop，用户需要自己下载 `flink-shaded` 工程源码，基于源码和自己的 Hadoop 版本自行编译生成依赖包。编译命令如下。

```bash
$ mvn clean install -Dhadoop.version=2.6.1
```

上面的命令编译了针对 Hadoop 2.6.1 的 `flink-shaded` 工程。编译完成后，将名为 `flink-shaded-hadoop-2-uber` 的依赖包添加到 Flink 主目录的 `lib` 目录中。
该方式没有依赖冲突的风险，但源码编译需要用户对 Maven 和 Hadoop 都有一定的了解。

## 本地调试

9.5.1 小节和 9.5.2 小节介绍的是针对 Flink 集群的 Hadoop 依赖设置方式，如果我们仅想在本地的 IntelliJ IDEA 里调试 Flink Hadoop 相关的程序，我们可以将下面的 Maven 依赖添加到 `pom.xml` 中。

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>2.8.3</version>
    <scope>provided</scope>
</dependency>
```