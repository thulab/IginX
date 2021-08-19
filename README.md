# IginX

- [**For support, please contact IginX members**](mailto:TSIginX@gmail.com)

## What is IginX?

[IginX](https://github.com/thulab/IginX) (Intelligent/IoTDB Engine X) is an open-source clustering
system for multi-dimensional scaling of standalone time series databases through generalized
sharding. It is emerged and evolved from the middleware
system [IKR](https://github.com/thulab/iotdb-kairosdb), already deployed in real applications.

IginX features in the following aspects:

- __High Scalability__

IginX is a stateless service. It can be easily scaled out or scaled up. You can expand the system's
routing and processing capacity by simply adding new IginX instances. Vertically scale-up the
computing resources for a single instance can also expand the system's capacity.

- __Smooth Elasticity__

With IginX you can even split and merge slices in multiple dimensions as your needs grow, with an
atomic cutover step that takes only a few seconds. Applications will rarely notice any performance
degradation in the process, thanks to the carefully tailored design of metadata and reconfiguration
procedure.

- __Transparent Data Distribution__

By encapsulating data slice orchestration logic, IginX allows application code and time series data
queries to remain agnostic to the distribution of data onto multiple slices. Users need only care
about the data access logic of their applications by IginX.

- __Integration with Heterogeneous Databases__

IginX provides a common abstraction of time series databases. As long as an implementation of the
abstraction is provided and configured for a time series database, it can be managed by and accessed
through IginX. Within a running cluster of IginX, heterogenous time series databases can coexist and
serve the same set of applications.

- __Flexible Slicing and Replication__

IginX allows for flexible data slicing and replication to suit the skewed application workloads,
which commonly exist in real world. This can be achieved through an implementation of the IPolicy
interface.

For more details, please refer to our technological posts on time series management
under [this link](https://github.com/thulab/IginX/wiki). However, IginX is still under active
development and yet to be mature. You are highly encourage to try it and share us with your
experience.

## Quick start

Quick starts in Chinese ([A complete version](./docs/quickStarts/IginXManual.md)——完整版部署说明文档): 

- [Use IginX in one shot](./docs/quickStarts/IginXInOneShot.md)：单机版IginX一键运行使用
- [Use IginX by compiling sources](./docs/quickStarts/IginXBySource.md)：基于代码编译使用IginX
- [Use IginX by docker](./docs/quickStarts/IginXByDocker.md)：基于 Docker 使用IginX
- [Deploy an IginX Cluster](./docs/quickStarts/IginXCluster.md)：IginX集群版部署

Or, please refer to our [User manual in Chinese](./docs/pdf/userManualC.pdf). User manual in English is
still being written.

### To understand technological designs in IginX

If you are interested in time series data management by large, you are highly welcomed to join our
IginX workshop in every month's last Friday afternoon at 2pm by Tencent online meeting. Please
contact the IginX-maintainers for information about the Tencent online meeting.

### To start developing IginX

Contributions are welcomed and greatly appreciated. To report a problem the best way to get
attention is to create a GitHub issue. To report a security vulnerability, please email
IginX-maintainers.

## Architecture

<img src="https://github.com/thulab/IginX/blob/main/docs/images/cluster_arch.png" width = "380" height = "300" alt="IginX cluster architecture" align=center />

## License

Unless otherwise noted, the IginX source files are distributed under the Apache Version 2.0 license
found in the LICENSE file.
