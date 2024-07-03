(flink-command-line-interface-guide)=
# 命令行工具

在生产环境中，Flink使用命令行工具（Command Line Interface）来管理作业的执行。命令行工具本质上是一个可执行脚本，名为flink，放置在Flink的主目录下的bin文件夹中。它的功能主要包括：提交、取消作业，罗列当前正在执行和排队的作业、获取某个作业的信息，设置Savepoint等。

命令行工具完成以上功能的前提是，我们已经启动了一个Flink集群，命令行工具能够直接连接到这个集群上。默认情况下，命令行工具会从conf/flink-conf.yaml里读取配置信息。

进入Flink主目录，在Linux命令行中输入`./bin/flink`，屏幕上会输出命令行工具的使用方法。其使用方法如下面的语法所示。

```bash
./bin/flink <ACTION> [OPTIONS] [ARGUMENTS]
```

其中，`ACTION`包括`run`、`stop`等，分别对应提交和取消作业。`OPTIONS`为一些预置的选项，`ARGUMENTS`是用户传入的参数。由于命令行工具的参数很多，我们只介绍一些经常使用的参数，其他参数可以参考Flink官方文档。

## 9.4.1 提交作业

提交作业的语法如下。

```bash
$ ./bin/flink run [OPTIONS] <xxx.jar> [ARGUMENTS]
```

我们要提供一个打包好的用户作业JAR包。打包需要使用Maven，在自己的Java工程目录下执行`mvn package`，在`target`文件夹下找到相应的JAR包。

我们使用Flink给我们提供的WordCount程序来演示。它的JAR包在Flink主目录下：`./examples/streaming/WordCount.jar`。提交作业的命令如下。

```bash
$ ./bin/flink run ./examples/streaming/WordCount.jar
```

任何一个Java程序都需要一个主类和main方法作为入口，启动WordCount程序时，我们并没有提及主类，因为程序在`pom.xml`文件中设置了主类。确切地说，经过Maven打包生成的JAR包有文件`META-INF/MANIFEST.MF`，该文件里定义了主类。如果我们想明确使用自己所需要的主类，可以使用`-c <classname>` 或`--class <classname>`来指定程序的主类。在一个包含众多`main()`方法的JAR包里，必须指定一个主类，否则会报错。

```bash
$ ./bin/flink run \
  -c org.apache.flink.streaming.examples.wordcount.WordCount \
  ./examples/streaming/WordCount.jar
```

我们也可以往程序中传入参数。

```bash
$ ./bin/flink run \
  -c org.apache.flink.streaming.examples.wordcount.WordCount \
  ./examples/streaming/WordCount.jar \
  --input '/tmp/a.log' \
  --output '/tmp/b.log'
```

其中，`--input '/tmp/a.log' --output '/tmp/b.log'`为我们传入的参数，和其他Java程序一样，这些参数会写入`main()`方法的参数`String[]`中，以字符串数组的形式存在。参数需要程序代码解析，因此命令行工具与程序代码中的参数要保持一致，否则会出现参数解析错误的情况。

我们也可以在命令行中用`-p`选项设置这个作业的并行度。下面的命令给作业设置的并行度为2。

```bash
$ ./bin/flink run -p 2 ./examples/streaming/WordCount.jar
```

如果用户在代码中使用`setParallelism()`方法明确设置并行度，或有给某个算子设置并行度，那么用户代码中的设置会覆盖命令行中的`-p`设置。

提交作业本质上是向Flink的Master提交JAR包，可以用`-m`选项来设置向具体哪个Master提交。下面的命令将作业提交到Hostname为`myJMHost`的节点上，端口号为8081。

```bash
$ ./bin/flink run \
  -m myJMHost:8081 \
  ./examples/streaming/WordCount.jar
```

如果我们已经启动了一个YARN集群，且当前节点可以连接到YARN集群上，`-m yarn-cluster`会将作业以Per-Job模式提交到YARN集群上。如果我们已经启动了一个Flink YARN Session，可以不用设置`-m`选项，Flink会记住Flink YARN Session的连接信息，默认向这个Flink YARN Session提交作业。

因为Flink支持不同类型的部署方式，为了避免提交作业的混乱、设置参数过多，Flink提出了`-e <arg>`或`--executor <arg>`选项，用户可以通过这两个选项选择使用哪种执行模式（Executor Mode）。可选的执行模式有：`remote`、`local`、`kubernetes-session`、`yarn-per-job`、 `yarn-session`。例如，一个原生Kubernetes Session中提交作业的命令如下。

```bash
$ ./bin/flink run \ 
  -e kubernetes-session \
  -Dkubernetes.cluster-id=<ClusterId> \
  examples/streaming/WindowJoin.jar
```

上面命令的`-D`用于设置参数。我们用`-D<property=value>`形式来设置一些配置信息，这些配置的含义和内容和`conf/flink-conf.yaml`中的配置是一致的。

无论用以上哪种方式提交作业，Flink都会将一些信息输出到屏幕上，最重要的信息就是作业的ID。

## 9.4.2 管理作业

罗列当前的作业的命令如下。

```bash
$ ./bin/flink list
```

触发一个作业执行Savepoint的命令如下。

```bash
$ ./bin/flink savepoint <jobId> [savepointDirectory]
```

这行命令会通知作业ID为`jobId`的作业执行Savepoint，可以在后面添加路径，Savepoint会写入对应目录，该路径必须是Flink Master可访问到的目录，例如一个HDFS路径。

关停一个Flink作业的命令如下。

```bash
$ ./bin/flink cancel <jobID>
```

关停一个带Savepoint的作业的命令如下。

```bash
$ ./bin/flink stop <jobID>
```

从一个Savepoint恢复一个作业的命令如下。

```bash
$ ./bin/flink run -s <savepointPath> [OPTIONS] <xxx.jar>
```