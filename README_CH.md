# c-raft

[![CI Tests](https://github.com/canonical/raft/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/canonical/raft/actions/workflows/build-and-test.yml) [![codecov](https://codecov.io/gh/canonical/raft/branch/master/graph/badge.svg)](https://codecov.io/gh/canonical/raft) [![Documentation Status](https://readthedocs.org/projects/raft/badge/?version=latest)](https://raft.readthedocs.io/en/latest/?badge=latest)

**注意**：中文文档有可能未及时更新，请以最新的英文[readme](./README.md)为准。

raft协议的完全异步C语言实现。

raft库采用模块化设计，核心部分实现了完全独立于平台的raft算法逻辑。重要的是为网络传输（发送/接收rpc消息）和存储持久化（日志和快照）提供了可插拔的I/O接口实现。

在使用默认选项构建库时，基于 [libuv](http://libuv.org)提供了 I/O 接口的库存实现，适合绝大多数应用场景。唯一的问题是它目前
需要 Linux，因为它使用 Linux [AIO](http://man7.org/linux/man-pages/man2/io_submit.2.html) API 用于磁盘输入/输出。欢迎添加补丁以支持更多的平台。

可以通过[raft.h](https://github.com/canonical/raft/blob/master/include/raft.h)来查看全部接口。

## license

这个 raft C 库是在略微修改的 LGPLv3 版本下发布的，其中包括一个版权例外，允许用户在他们的项目中静态链接这个库的代码并按照自己的条款发布最终作品。如有需要，请查看完整[license](https://github.com/canonical/raft/blob/LICENSE)文件。

## Features

该实现包括 Raft 论文中描述的所有基本功能：

- 领导选举

- 日志复制

- 日志压缩

- 成员变更

它还包括一些可选的增强功能：

- 优化流水线以减少日志复制的延迟
- 并行写入领导者的磁盘
- 领导者失去法定人数时自动下台
- 领导权转移扩展
- 预投票协议

## install

如果您使用的是基于 Debian 的系统，您可以从 dqlite 的[dev PPA](https://launchpad.net/~dqlite/+archive/ubuntu/dev) 获得最新的开发版本：

```bash
sudo add-apt-repository ppa:dqlite/dev
sudo apt-get update
sudo apt-get install libraft-dev
```

## Building

从源码编译libraft，需要准备：

- 最较新版本的[libuv](https://libuv.org/)（v1.18.0或之后的版本）
- 可选的[liblz4](https://lz4.github.io/lz4/)（推荐使用）（v1.7.1或之后的版本）

```bash
sudo apt-get install libuv1-dev liblz4-dev

autoreconf -i

./configure --enable-example

make
```

## Example

理解如何使用raft库的最好方式是阅读源码目录下的[example server](https://github.com/canonical/raft/blob/master/example/server.c)。

可以运行如下命令来了解example server的运行情况：

```bash
./example/cluster
```

这个命令启动了一个由3节点组成的集群，并且会不断的随机停止一个节点并重启。

## Quick guide

以下是一个如何使用raft库的快速指南（为了简洁起见，省略了错误处理），要想了解详细的信息建议阅读[raft.h](https://github.com/canonical/raft/blob/master/include/raft.h)。

1. 创建一个`raft_io` 接口实现的实例（如果库附带的接口确实不适合，则实现您自己的实例）：

```c
const char *dir = "/your/raft/data";
struct uv_loop_s loop;
struct raft_uv_transport transport;
struct raft_io io;
uv_loop_init(&loop);
raft_uv_tcp_init(&transport, &loop);
raft_uv_init(&io, &loop, dir, &transport);
```

2. 定义应用层的`Raft FSM`，实现`raft_fsm`接口

````c
```C
struct raft_fsm
{
  void *data;
  int (*apply)(struct raft_fsm *fsm, const struct raft_buffer *buf, void **result);
  int (*snapshot)(struct raft_fsm *fsm, struct raft_buffer *bufs[], unsigned *n_bufs);
  int (*restore)(struct raft_fsm *fsm, struct raft_buffer *buf);
}
```
````

3. 为每个服务节点选择一个唯一的 ID 和地址并初始化 raft 对象：

```C
unsigned id = 1;
const char *address = "192.168.1.1:9999";
struct raft raft;
raft_init(&raft, &io, &fsm, id, address);
```

4. 如果这是您第一次启动集群，请创建一个包含集群中服务器节点（节点通常只有一个，因为稍后可以使用`raft_add `和`raft_promote `扩展集群）的配置对象，并启动配置：

```c
struct raft_configuration configuration;
raft_configuration_init(&configuration);
raft_configuration_add(&configuration, 1, "192.168.1.1:9999", true);
raft_bootstrap(&raft, &configuration);
```

5. 启动raft服务器节点

```c
raft_start(&raft);
uv_run(&loop, UV_RUN_DEFAULT);
```

6. 异步提交请求将新命令应用到应用程序状态机

```c
static void apply_callback(struct raft_apply *req, int status, void *result) {
  /* ... */
}

struct raft_apply req;
struct raft_buffer buf;
buf.len = ...; /* The length of your FSM entry data */
buf.base = ...; /* Your FSM entry data */
raft_apply(&raft, &req, &buf, 1, apply_callback);
```

7. 添加更多的服务器节点到集群使用```raft_add()``` 和```raft_promote``` APIs

## Usage Notes

默认基于 [libuv](http://libuv.org) 的 `raft_io` 实现使用 `liblz4 `库压缩 raft 快照。 除了节省磁盘空间，`lz4 `压缩快照以[内容校验和](https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md)的形式提供额外的数据完整性检查，这允许 `raft`检测存储期间发生的损坏。 因此，建议不要通过`--disable-lz4` 配置标志禁用 `lz4 `压缩。

当环境变量LIBRAFT_TRACE在启动时被设置，将启用详细跟踪。

## Notable users

\- [dqlite](https://github.com/canonical/dqlite)

## Credits

当然，最感谢的是 `Diego Ongaro` :)（raft论文的作者）。下面的raft实现提供了很多灵感和想法：

- CoreOS' Go implementation for [etcd](https://github.com/etcd-io/etcd/tree/master/raft)

- Hashicorp's Go [raft](https://github.com/hashicorp/raft)

- Willem's [C implementation](https://github.com/willemt/raft)

- LogCabin's [C++ implementation](https://github.com/logcabin/logcabin)