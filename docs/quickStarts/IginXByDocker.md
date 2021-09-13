# IginX 安装使用教程（Docker运行）

IginX 是清华大学大数据系统软件国家工程实验室，为满足工业互联网场景推出的新一代高可扩展时序数据库分布式中间件，目前支持 IoTDB，InfluxDB 作为数据后端。

## 环境安装

### Java 安装

由于 ZooKeeper、IginX 以及 IoTDB 都是使用 Java 开发的，因此首先需要安装 Java。如果本地已经安装了 JDK>=1.8 的运行环境，**直接跳过此步骤**。

1. 首先访问 [Java官方网站](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html)下载适用于当前系统的 JDK 包。
2. 安装

```shell
$ cd ~/Downloads
$ tar -zxf jdk-8u181-linux-x64.gz # 解压文件
$ mkdir /opt/jdk
$ mv jdk-1.8.0_181 /opt/jdk/
```

3. 设置路径

编辑 ~/.bashrc 文件，在文件末端加入如下的两行：

```shell
export JAVA_HOME = /usr/jdk/jdk-1.8.0_181
export PATH=$PATH:$JAVA_HOME/bin
```

加载更改后的配置文件：

```shell
$ source ~/.bashrc
```

4. 使用 java -version 判断 JDK 是否安装成功

```shell
$ java -version
java version "1.8.0_181"
Java(TM) SE Runtime Environment (build 1.8.0_181-b13)
Java HotSpot(TM) 64-Bit Server VM (build 25.181-b13, mixed mode)
```

如果显示出如上的字样，则表示安装成功。

### Maven 安装

Maven 是 Java 项目管理和自动构建工具，如果您需要从源码进行编译，还需要安装 Maven >= 3.6 的环境，否则，**直接跳过此步骤**。

1. 访问[官网](http://maven.apache.org/download.cgi)下载并解压 Maven

```
$ wget http://mirrors.hust.edu.cn/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
$ tar -xvf  apache-maven-3.3.9-bin.tar.gz
$ sudo mv -f apache-maven-3.3.9 /usr/local/
```

2. 设置路径

编辑 ~/.bashrc 文件，在文件末端加入如下的两行：

```shell
export MAVEN_HOME=/usr/local/apache-maven-3.3.9
export PATH=${PATH}:${MAVEN_HOME}/bin
```

加载更改后的配置文件：

```shell
$ source ~/.bashrc
```

3. 使用 mvn -v 判断 Maven 是否安装成功

```shell
$ mvn -v
Apache Maven 3.6.1 (d66c9c0b3152b2e69ee9bac180bb8fcc8e6af555; 2019-04-05T03:00:29+08:00)
```

如果显示出如上的字样，则表示安装成功。

### Docker 安装

docker 官方提供了安装脚本，允许用户使用命令自动安装：

```shell
$ curl -fsSL https://get.docker.com | bash -s docker --mirror Aliyun
```

也可以使用国内 daocloud 一键安装命令：

```shell
$ curl -sSL https://get.daocloud.io/docker | sh
```

运行命令即可启动 docker engine，并查看 docker 版本：

```shell
$ systemctl start docker
$ docker version
```

显示出如下字样，表示 docker 安装成功：

```shell
Client: Docker Engine - Community
 Version:           20.10.8
 API version:       1.41
 Go version:        go1.16.6
 Git commit:        3967b7d
 Built:             Fri Jul 30 19:55:49 2021
 OS/Arch:           linux/amd64
 Context:           default
 Experimental:      true

Server: Docker Engine - Community
 Engine:
  Version:          20.10.8
  API version:      1.41 (minimum version 1.12)
  Go version:       go1.16.6
  Git commit:       75249d8
  Built:            Fri Jul 30 19:54:13 2021
  OS/Arch:          linux/amd64
  Experimental:     false
 containerd:
  Version:          1.4.9
  GitCommit:        e25210fe30a0a703442421b0f60afac609f950a3
 runc:
  Version:          1.0.1
  GitCommit:        v1.0.1-0-g4144b63
 docker-init:
  Version:          0.19.0
  GitCommit:        de40ad0
```

## 编译镜像

目前 IginX 的 docker 镜像需要手动安装到本地。首先需要下载 Iginx 源码并编译：

```shell
$ cd ~
$ git clone git@github.com:thulab/IginX.git # 拉取最新的 IginX 代码
$ cd IginX
$ mvn clean install -Dmaven.test.skip=true # 编译 IginX
```

显示出如下字样表示编译成功：

```shell
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for IginX 0.3.0-SNAPSHOT:
[INFO]
[INFO] IginX .............................................. SUCCESS [  0.274 s]
[INFO] IginX Thrift ....................................... SUCCESS [  6.484 s]
[INFO] IginX Shared ....................................... SUCCESS [  1.015 s]
[INFO] IginX Session ...................................... SUCCESS [  0.713 s]
[INFO] IginX Antlr ........................................ SUCCESS [  1.654 s]
[INFO] IginX Core ......................................... SUCCESS [  9.471 s]
[INFO] IginX IoTDB ........................................ SUCCESS [  1.234 s]
[INFO] IginX InfluxDB ..................................... SUCCESS [  0.823 s]
[INFO] IginX Client ....................................... SUCCESS [  3.045 s]
[INFO] IginX JDBC ......................................... SUCCESS [  0.802 s]
[INFO] IginX Example ...................................... SUCCESS [  0.606 s]
[INFO] IginX Test ......................................... SUCCESS [  0.116 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  26.432 s
[INFO] Finished at: 2021-08-19T11:06:12+08:00
[INFO] ------------------------------------------------------------------------

```

随后使用 maven 插件构建 IginX 镜像：

```shell
$ mvn package shade:shade -pl core -DskipTests docker:build
```

显示出如下的字样表示镜像构建成功：

```shell
Successfully tagged iginx:latest
[INFO] Built iginx
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  21.268 s
[INFO] Finished at: 2021-08-19T11:08:46+08:00
[INFO] ------------------------------------------------------------------------
```

可以使用 docker 命令查看已经安装到本地的 IginX 镜像：

```shell
$ docker images
REPOSITORY					TAG					IMAGE ID					CREATED					SIZE
iginx								latest			7f00b2b9510b			36 seconds ago	702MB
```

## 基于镜像运行

首先需要从 DockerHub 上拉取 IoTDB 镜像：

```shell
$ docker pull apache/iotdb:0.11.4
0.11.4: Pulling from apache/iotdb
a076a628af6f: Pull complete
943d8acaac04: Downloading
b9998d19c116: Download complete
8162353a3d61: Waiting
af85646a1131: Waiting
0.11.4: Pulling from apache/iotdb
a076a628af6f: Pull complete
943d8acaac04: Pull complete
b9998d19c116: Pull complete
8162353a3d61: Pull complete
af85646a1131: Pull complete
Digest: sha256:a29a48297e6785ac13abe63b4d06bdf77d50203c13be98b773b2cfb07b76d135
Status: Downloaded newer image for apache/iotdb:0.11.4
docker.io/apache/iotdb:0.11.4
```

考虑到 IginX 和 IoTDB 之前通过网络进行通讯，因此需要建立 Docker 网络，允许其通过网络互联。在这里我们创建一个名为 iot 的 bridge 网络：

```shell
$ docker network create -d bridge iot
```

然后启动一个 IoTDB 实例：

```shell
$ docker run -d --name iotdb --network iot apache/iotdb:0.11.4
```

最后启动 IginX，选择使用本地文件作为元数据存储后端，并设置后端存储为刚刚启动的 IoTDB 实例即可完成整个系统的启动：

```shell
$ docker run -e "metaStorage=file" -e "storageEngineList=iotdb#6667#iotdb#sessionPoolSize=20" -p 6888:6888 --network iot -it iginx
```

该命令会将本地的 6888 接口暴露出来，作为与 IginX 集群的通讯接口。

在系统启动完毕后，可以运行 IginX 源码 example 目录下的 cn.edu.tsinghua.iginx.session.IoTDBSessionExample 类，往其中尝试插入/读取数据。

> 关于 IginX docker 版本的参数设置：
>
> 通常情况下，IginX 使用本地文件作为配置参数的来源。不过在 main 分支最新的版本，支持使用环境变量来配置参数。
>
> 例如配置文件中的参数 port，表示系统的启动端口，在配置文件中指定为 6888，即 IginX 对外暴露的端口为 6888。
>
> ```shell
> port=6888
> ```
>
> 可以设置环境变量 port 为其他值，例如 6889，那么在此情况下 IginX 的启动端口将变更为 6889。
>
> ```shell
>  export port=6889
> ```
>
> 设置在环境变量的参数会覆盖掉配置文件中的参数。
>
> 在 Docker 环境中，更改配置文件的方式较为繁琐，因此推荐使用设置 Docker 容器环境变量的方式来配置系统参数。

