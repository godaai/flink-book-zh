(sec-deployment-and-configuration)=
# Flink 集群部署模式

当前，信息系统基础设施正在飞速发展，常见的基础设施包括物理机集群、虚拟机集群、容器集群等。为了兼容这些基础设施，Flink 曾在 1.7 版本中做了重构，提出了第 3 章中所示的 Master-Worker 架构，该架构可以兼容几乎所有主流信息系统的基础设施，包括 Standalone 集群、Hadoop YARN 集群或 Kubernetes 集群。

## Standalone 集群

一个 Standalone 集群包括至少一个 Master 进程和至少一个 TaskManager 进程，每个进程作为一个单独的 Java JVM 进程。其中，Master 节点上运行 Dispatcher、ResourceManager 和 JobManager，Worker 节点将运行 TaskManager。{numref}`fig-flink-standalone-cluster` 展示了一个 4 节点的 Standalone 集群，其中，IP 地址为 192.168.0.1 的节点为 Master 节点，其他 3 个为 Worker 节点。

```{figure} ./img/Flink-Standalone-cluster.png
---
name: fig-flink-standalone-cluster
width: 80%
align: center
---
Flink Standalone 集群
```

第 2 章的实验中，我们已经展示了如何下载和解压 Flink，该集群只部署在本地，结合图 9-1，本节介绍如何在一个物理机集群上部署 Standalone 集群。我们可以将解压后的 Flink 主目录复制到所有节点的相同路径上；也可以在一个共享存储空间（例如 NFS）的路径上部署 Flink，所有节点均可以像访问本地目录那样访问共享存储上的 Flink 主目录。此外，节点之间必须实现免密码登录：基于安全外壳协议（Secure Shell，SSH），将公钥拷贝到待目标节点，可以实现节点之间免密码登录。所有节点上必须提前安装并配置好 JDK，将 $JAVA_HOME 放入环境变量。

我们需要编辑 `conf/flink-conf.yaml` 文件，将 `jobmanager.rpc.address` 配置为 Master 节点的 IP 地址 192.168.0.1；编辑 `conf/slaves` 文件，将 192.168.0.2、192.168.0.3 和 192.168.0.4 等 Worker 节点的 IP 地址加入该文件中。如果每个节点除了 IP 地址外，还配有主机名（Hostname），我们也可以用 Hostname 替代 IP 地址来做上述配置。

综上，配置一个 Standalone 集群需要注意以下几点：
- 为每台节点分配固定的 IP 地址，或者配置 Hostname，节点之间设置免密码 SSH 登录。
- 在所有节点上提前安装配置 JDK，将 `$JAVA_HOME` 添加到环境变量中。
- 配置 `conf/flink-conf.yaml` 文件，设置 `jobmanager.rpc.address` 为 Master 节点的 IP 地址或 Hostname。配置 `conf/slaves` 文件，将 Worker 节点的 IP 地址或 Hostname 添加进去。
- 将 Flink 主目录同步到所有节点的相同目录下，或者部署在一个共享目录上，共享目录可被所有节点访问。

接着，我们回到 Master 节点，进入 Flink 主目录，运行 `bin/start-cluster.sh`。该脚本会在 Master 节点启动 Master 进程，同时读取 `conf/slaves` 文件，脚本会帮我们 SSH 登录到各节点上，启动 TaskManager。至此，我们启动了一个 Flink Standalone 集群，我们可以使用 Flink Client 向该集群的 Master 节点提交作业。

```bash
$ ./bin/flink run -m 192.168.0.1:8081 ./examples/batch/WordCount.jar
```

可以使用 `bin/stop-cluster.sh` 脚本关停整个集群。

## Hadoop YARN 集群

Hadoop 一直是很多公司首选的大数据基础架构，YARN 也是经常使用的资源调度器。YARN 可以管理一个集群的 CPU 和内存等资源，MapReduce、Hive 或 Spark 都可以向 YARN 申请资源。YARN 中的基本调度资源是容器（Container）。

注意：
YARN Container 和 Docker Container 有所不同。YARN Container 只适合 JVM 上的资源隔离，Docker Container 则是更广泛意义上的 Container。

为了让 Flink 运行在 YARN 上，需要提前配置 Hadoop 和 YARN，这包括下载针对 Hadoop 的 Flink，设置 `HADOOP_CONF_DIR` 和 `YARN_CONF_DIR` 等与 Hadoop 相关的配置，启动 YARN 等。网络上有大量相关教程，这里不赘述 Hadoop 和 YARN 的安装方法，但是用户需要按照 9.5 节介绍的内容来配置 Hadoop 相关依赖。

在 YARN 上使用 Flink 有 3 种模式：Per-Job 模式、Session 模式和 Application 模式。Per-Job 模式指每次向 YARN 提交一个作业，YARN 为这个作业单独分配资源，基于这些资源启动一个 Flink 集群，该作业运行结束后，相应的资源会被释放。Session 模式在 YARN 上启动一个长期运行的 Flink 集群，用户可以向这个集群提交多个作业。Application 模式在 Per-Job 模式上做了一些优化。{numref}`fig-per-job-submission` 展示了 Per-Job 模式的作业提交流程。

```{figure} ./img/Per-Job.png
---
name: fig-per-job-submission
width: 80%
align: center
---
Per-Job 模式的作业提交流程
```

Client 首先将作业提交给 YARN 的 ResourceManager，YARN 为这个作业生成一个 ApplicationMaster 以运行 Fink Master，ApplicationMaster 是 YARN 中承担作业资源管理等功能的组件。ApplicationMaster 中运行着 JobManager 和 Flink-YARN ResourceManager。JobManager 会根据本次作业所需资源向 Flink-YARN ResourceManager 申请 Slot 资源。

:::{note}
这里有两个 ResourceManager，一个是 YARN 的 ResourceManager，它是 YARN 的组件，不属于 Flink，它负责整个 YARN 集群全局层面的资源管理和任务调度；一个是 Flink-YARN ResourceManager，它是 Flink 的组件，它负责当前 Flink 作业的资源管理。
:::

Flink-YARN ResourceManager 会向 YARN 申请所需的 Container，YARN 为之分配足够的 Container 作为 TaskManager。TaskManager 里有 Flink 计算所需的 Slot，TaskManager 将这些 Slot 注册到 Flink-YARN ResourceManager 中。注册成功后，JobManager 将作业的计算任务部署到各 TaskManager 上。

下面的命令使用 Per-Job 模式启动单个作业。

```bash
$ ./bin/flink run -m yarn-cluster ./examples/batch/WordCount.jar
```

`-m yarn-cluster` 表示该作业使用 Per-Job 模式运行在 YARN 上。

{numref}`fig-session-submission` 展示了 Session 模式的作业提交流程。

```{figure} ./img/session.png
---
name: fig-session-submission
width: 80%
align: center
---
Session 模式的作业提交流程
```

Session 模式将在 YARN 上启动一个 Flink 集群，用户可以向该集群提交多个作业。

首先，我们在 Client 上，用 `bin/yarn-session.sh` 启动一个 YARN Session。Flink 会先向 YARN ResourceManager 申请一个 ApplicationMaster，里面运行着 Dispatcher 和 Flink-YARN ResourceManager，这两个组件将长期对外提供服务。当提交一个具体的作业时，作业相关信息被发送给了 Dispatcher，Dispatcher 会启动针对该作业的 JobManager。

接下来的流程就与 Per-Job 模式几乎一模一样：JobManager 申请 Slot，Flink-YARN ResourceManager 向 YARN 申请所需的 Container，每个 Container 里启动 TaskManager，TaskManager 向 Flink-YARN ResourceManager 注册 Slot，注册成功后，JobManager 将计算任务部署到各 TaskManager 上。如果用户提交下一个作业，那么 Dispatcher 启动新的 JobManager，新的 JobManager 负责新作业的资源申请和任务调度。

下面的命令本启动了一个 Session，该 Session 的 JobManager 内存大小为 1024MB，TaskManager 内存大小为 4096MB。

```bash
$ ./bin/yarn-session.sh -jm 1024m -tm 4096m
```

启动后，屏幕上会显示 Flink WebUI 的连接信息。例如，在一个本地部署的 YARN 集群上创建一个 Session 后，假设分配的 WebUI 地址为：`http://192.168.31.167:54680/`。将地址复制到浏览器，打开即显示 Flink WebUI。

之后我们可以使用 `bin/flink` 在该 Session 上启动一个作业。

```bash
$ ./bin/flink run ./examples/batch/WordCount.jar
```

上述提交作业的命令没有特意指定连接信息，所提交的作业会直接在 Session 中运行，这是因为 Flink 已经将 Session 的连接信息记录了下来。从 Flink WebUI 页面上可以看到，刚开始启动时，UI 上显示 Total/Available Task Slots 为 0，Task Managers 也为 0。随着作业的提交，资源会动态增加：每提交一个新的作业，Flink-YARN ResourceManager 会动态地向 YARN ResourceManager 申请资源。

比较 Per-Job 模式和 Session 模式发现：Per-Job 模式下，一个作业运行完后，JobManager、TaskManager 都会退出，Container 资源会释放，作业会在资源申请和释放上消耗时间；Session 模式下，Dispatcher 和 Flink-YARN ResourceManager 是可以被多个作业复用的。无论哪种模式，每个作业都有一个 JobManager 与之对应，该 JobManager 负责单个作业的资源申请、任务调度、Checkpoint 等协调性功能。Per-Job 模式更适合长时间运行的作业，作业对启动时间不敏感，一般是长期运行的流处理任务。Session 模式更适合短时间运行的作业，一般是批处理任务。

除了 Per-Job 模式和 Session 模式，Flink 还提供了一个 Application 模式。Per-Job 和 Session 模式作业提交的过程比较依赖 Client，一个作业的 main()方法是在 Client 上执行的。main() 方法会将作业的各个依赖下载到本地，生成 JobGraph，并将依赖以及 JobGraph 发送到 Flink 集群。在 Client 上执行 main()方法会导致 Client 的负载很重，因为下载依赖和将依赖打包发送到 Flink 集群都对网络带宽有一定要求，执行 main() 方法会加重 CPU 的负担。而且在很多企业，多个用户会共享一个 Client，多人共用加重了 Client 的压力。为了解决这个问题，Flink 的 Application 模式允许 main() 方法在 JobManager 上执行，这样可以分担 Client 的压力。在资源隔离层面上，Application 模式与 Per-Job 模式基本一样，相当于为每个作业应用创建一个 Flink 集群。

具体而言，我们可以用下面的代码，基于 Application 模式提交作业。

```bash
$ ./bin/flink run-application -t yarn-application \
  -Djobmanager.memory.process.size=2048m \
  -Dtaskmanager.memory.process.size=4096m \
  -Dyarn.provided.lib.dirs="hdfs://myhdfs/my-remote-flink-dist-dir" \
  ./examples/batch/WordCount.jar
```

在上面这段提交作业的代码中，`run-application` 表示使用 Application 模式，`-D` 前缀加上参数配置来设置一些参数，这与 Per-Job 模式和 Session 模式的参数设置稍有不同。为了让作业下载各种依赖，可以向 HDFS 上传一些常用的 JAR 包，本例中上传路径是 `hdfs://myhdfs/my-remote-flink-dist-dir`，然后使用 `-Dyarn.provided.lib.dirs` 告知 Flink 上传 JAR 包的地址，Flink 的 JobManager 会前往这个地址下载各种依赖。

## Kubernetes 集群

Kubernetes（简称 K8s）是一个开源的 Container 编排平台。近年来，Container 以及 Kubernetes 大行其道，获得了业界的广泛关注，很多信息系统正在逐渐将业务迁移到 Kubernetes 上。

在 Flink 1.10 之前，Flink 的 Kubernetes 部署需要用户对 Kubernetes 各组件和工具有一定的了解，而 Kubernetes 涉及的组件和概念较多，学习成本较高。和 YARN 一样，Flink Kubernetes 部署方式支持 Per-Job 和 Session 两种模式。为了进一步减小 Kubernetes 部署的难度，Flink 1.10 提出了原生 Kubernetes 部署，同时也保留了之前的模式。新的 Kubernetes 部署非常简单，将会成为未来的趋势，因此本小节只介绍这种原生 Kubernetes 部署方式。

注意：
原生 Kubernetes 部署是 Flink 1.10 推出的新功能，还在持续迭代中，一些配置文件和命令行参数有可能在未来的版本迭代中发生变化，读者使用前最好阅读最新的官方文档。

在使用 Kubernetes 之前，需要确保 Kubernetes 版本为 1.9 以上，配置 `~/.kube/config` 文件，提前创建用户，并赋予相应权限。

Flink 原生 Kubernetes 部署目前支持 Session 模式和 Application 模式。Session 模式是在 Kubernetes 集群上启动 Session，然后在 Session 中提交多个作业。未来的版本将支持原生 Kubernetes Per-Job 模式。{numref}`fig-kubernetes-session-submission` 所示为一个原生 Kubernetes Session 模式的作业提交流程。

```{figure} ./img/Kubernetes-Session.png
---
name: fig-kubernetes-session-submission
width: 80%
align: center
---
原生 Kubernetes Session 模式的作业提交流程
```

如 {numref}`fig-kubernetes-session-submission` 中所示的第 1 步，我们用 `bin/kubernetes-session.sh` 启动一个 Kubernetes Session，Kubernetes 相关组件将进行初始化，Kubernetes Master、ConfigMap 和 Kubernetes Service 等模块生成相关配置，剩下的流程与 YARN 的 Session 模式几乎一致。Client 提交作业到 Dispatcher，Dispatcher 启动一个 JobManager，JobManager 向 Flink-Kubernetes ResourceManager 申请 Slot，Flink-Kubernetes ResourceManager 进而向 Kubernetes Master 申请资源。Kubernetes Master 分配资源，启动 Kubernetes Pod，运行 TaskManager，TaskManager 向 Flink-Kubernetes ResourceManager 注册 Slot，这个作业可以基于这些资源进行部署。

如 {numref}`fig-kubernetes-session-submission` 中所示的第 1 步，我们需要启动一个 Flink Kubernetes Session，其他参数需要参考 Flink 官方文档中的说明，相关命令如下。

```bash
$ ./bin/kubernetes-session.sh \
  -Dkubernetes.cluster-id=<ClusterId> \
  -Dkubernetes.container.image=<image> \
  -Dtaskmanager.memory.process.size=4096m \
  -Dkubernetes.taskmanager.cpu=2 \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dresourcemanager.taskmanager-timeout=3600000
```

上面的命令启动了一个名为 ClusterId 的 Flink Kubernetes Session 集群，集群中的每个 TaskManager 有 2 个 CPU、4096MB 的内存、4 个 Slot。ClusterId 是该 Flink Kubernetes Session 集群的标识，实际使用时我们需要设置一个名字，如果不进行设置，Flink 会给我们分配一个名字。

为了使用 Flink WebUI，可以使用下面的命令进行端口转发。

```bash
$ kubectl port-forward service/<ClusterID> 8081
```

在浏览器中打开地址 `http://127.0.0.1:8001`，就能看到 Flink 的 WebUI 了。与 Flink YARN Session 一样，刚开始所有的资源都是 0，随着作业的提交，Flink 会动态地向 Kubernetes 申请更多资源。

我们继续使用 `bin/flink` 向这个 Session 集群中提交作业。

```bash
$ ./bin/flink run -d -e kubernetes-session \
  -Dkubernetes.cluster-id=<ClusterId> examples/streaming/WindowJoin.jar
```

可以使用下面的命令关停这个 Flink Kubernetes Session 集群。

```bash
$ echo 'stop' | ./bin/kubernetes-session.sh \
  -Dkubernetes.cluster-id=<ClusterId> \
  -Dexecution.attached=true
```

原生 Kubernetes 也有 Application 模式，Kubernetes Application 模式与 YARN Application 模式类似。使用时，需要先将作业打成 JAR 包，放到 Docker 镜像中，代码如下。

```dockerfile
FROM flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY /path/of/my-flink-job-*.jar /opt/flink/usrlib/my-flink-job.jar
```

然后使用下面的代码行提交作业。

```bash
$ ./bin/flink run-application -p 8 -t kubernetes-application \
  -Dkubernetes.cluster-id=<ClusterId> \
  -Dtaskmanager.memory.process.size=4096m \
  -Dkubernetes.taskmanager.cpu=2 \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dkubernetes.container.image=<CustomImageName> \
  local:///opt/flink/usrlib/my-flink-job.jar
```

其中，`-Dkubernetes.container.image` 用来配置自定义的镜像，`local:///opt/flink/usrlib/my-flink-job.jar` 表示 JAR 包在镜像中的位置。
