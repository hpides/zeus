# HDES - A Dynamic Stream Processing Engine

We present HDES, a novel stream processing engine (SPE). The proposed engine focuses on enabling ad-hoc queries to allow end-users to dynamically add and remove queries. While other approaches added ad-hoc functionality on top of common open-source SPEs, such as AStream and AJoin, HDES aims to make dynamicity a first-class citizen. This change in perspective allows us to optimize this use-case upfront and elevates the performance of ad-hoc usage.

Moreover, we include several resource sharing optimizations into HDES to increase the performance of join-operations.

## Build

HDES can be built with Maven:

```shell script
mvn package -f ./engine
```

Note: The engine itself is not executable. For an example of how to use HDES, you can refer to our benchmark.

## Documentation

For an in-depth documentation, please refer to our [report](HDES_report.pdf).

## Evaluation
All our experiments can be rerun by executing the python scripts in the corresponding directory.
The [benchmark README](benchmark/README.md) contains an exhaustive list of CLI parameters the benchmark supports.

