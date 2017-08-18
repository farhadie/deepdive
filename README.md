# DeepDive

[![Build Status](https://travis-ci.org/HazyResearch/deepdive.svg?branch=master)](https://travis-ci.org/HazyResearch/deepdive)
[![Join the chat at https://gitter.im/HazyResearch/deepdive](https://badges.gitter.im/HazyResearch/deepdive.svg)](https://gitter.im/HazyResearch/deepdive?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

<strong><big>See [deepdive.stanford.edu](http://deepdive.stanford.edu) or [doc/](doc/index.md) to learn how to install and use DeepDive.</big></strong>

<strong>Or, just start with this one-liner command:</strong>
```bash
bash <(curl -fsSL git.io/getdeepdive)
```

Read the [DeepDive developer's guide](doc/developer.md#readme) to learn more about this source tree and how to contribute.

Licensed under [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.txt).


---
# Spark extended version
we added:
- A HDFS/Spark connector instead of a db-driver, we connect DeepDive to HDFS and Spark.
- A new Compute-driver, we can send UDFs to Spark cluster.
- Spark UDF wrapper for wrappering UDFs, for loading datasets from HDFS into Spark session and storing results into HDFS with correct format.
- A Spark wrapper for ddlib.

