![Flambo](http://static1.wikia.nocookie.net/__cb20120216165717/adventuretimewithfinnandjake/images/e/ee/Flambos_fire_magic.jpg)

# Flambo

Flambo is a Clojure DSL for [Apache Spark](http://spark.apache.org/docs/latest/)

**Contents**

* [Overview](#overview)
* [Supported Spark Versions](#versions)
* [Installation](#installation)
* [AOT](#aot)
* [Usage](#usage)
  * [Initializing flambo](#initializing-flambo)
  * [Resilient Distributed Datasets](#rdds)
  * [RDD Operations](#rdd-operations)
  * [RDD Persistence](#rdd-persistence)
* [Standalone Applications](#running-flambo)
* [Kryo](#kryo)
* [Acknowledgements](#acknowledgements)

<a name="overview">
## Overview

Apache Spark is a fast and general-purpose cluster computing system. It provides high-level APIs in Java, Scala and Python, and an optimized engine that supports general execution graphs.

Flambo is a Clojure DSL for Spark. It allows you to create and manipulate Spark data structures using idiomatic Clojure.

"So that's where I came from." --Flambo

<a name="versions">
## Supported Spark Versions

flambo 0.4.0 targets >= Spark 1.1.0

flambo 0.3.3 targets >= Spark 1.0.0

flambo 0.2.0 targets Spark 0.9.1

<a name="installation">
## Installation

Flambo is available from clojars. Depending on the version of Spark you're using, add one of the following to the dependences in your `project.clj` file:

### With Leiningen

`[yieldbot/flambo "0.4.0"]` for Spark 1.1.0 or greater

`[yieldbot/flambo "0.3.3"]` for Spark 1.0.0 or greater

`[yieldbot/flambo "0.2.0"]` for Spark 0.9.1

Don't forget to add spark (and possibly your hadoop distribution's hadoop-client library) to the `:provided` profile in your `project.clj` file:

```clojure
{:profiles {:provided
             {:dependencies
              [[org.apache.spark/spark-core_2.10 "1.1.1"]]}}}
```

<a name="aot">
## AOT

It is necessary to AOT compile any namespaces which require `flambo.api`. You can AOT compile your application uberjar before running it in your spark cluster. This can easily accomplished by adding an `:uberjar` profile with `{:aot :all}` in it.

When working locally in a REPL, you'll want to AOT compile those namespaces as well. An easy way to do that is to add an `:aot` key to your `:dev` profile in your leiningen project.clj

```clojure
:profiles {:dev
    {:aot [my.namespace my.other.namespace]}}
```

<a name="usage">
## Usage

Flambo makes developing Spark applications quick and painless by utilizing the powerful abstractions available in Clojure. For instance, you can use the Clojure threading macro `->` to chain sequences of operations and transformations.

<a name="initializing-flambo">
### Initializing flambo

The first step is to create a Spark configuration object, SparkConf, which contains information about your application. This is used to construct a SparkContext object which tells Spark how to access a cluster.

Here we create a SparkConf object with the string `local` to run in local mode:

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.conf :as conf])
  (:require [flambo.api :as f]))

(def c (-> (conf/spark-conf)
           (conf/master "local")
           (conf/app-name "flame_princess")))

(def sc (f/spark-context c))
```

The `master` url string parameter can be one of the following formats:

|  Master URL         | Meaning                                                                                                   |
|---------------------|-----------------------------------------------------------------------------------------------------------|
| `spark://HOST:PORT` | Connect to a [standalone Spark cluster](https://spark.apache.org/docs/0.9.1/spark-standalone.html) master.|
| `mesos://HOST:PORT` | Connect to a [Mesos](https://spark.apache.org/docs/0.9.1/running-on-mesos.html) cluster.                  |
| `local`             | Use one worker thread to run Spark locally (no parallelism).                                              |
| `local[N]`          | Use `N` worker threads to run Spark locally.                                                              |
| `local[*]`          | Use the same number of threads as cores to run Spark locally. <br> _Only_ available for Spark 1.0.0+      |

For running on YARN, see [running on YARN](https://spark.apache.org/docs/0.9.1/running-on-yarn.html) for details.

Hard-coding the value of `master` and other configuration parameters can be avoided by passing the values to Spark when running `spark-submit` (Spark 1.0.0) or by allowing `spark-submit` to read these properties from a configuration file. See [Standalone Applications](#running-flambo) for information on running flambo applications and see Spark's [documentation](http://spark.apache.org/docs/latest/configuration.html) for more details about configuring Spark properties.

<a name="rdds">
### Resilient Distributed Datasets (RDDs)

The main abstraction Spark provides is a _resilient distributed dataset_, RDD, which is a fault-tolerant collection of elements partitioned across the nodes of the cluster that can be operated on in parallel. There are two ways to create RDDs: _parallelizing_ an existing collection in your driver program, or referencing a dataset in an external storage system, such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.

#### Parallelized Collections

Parallelized collections (RDDs) in flambo are created by calling the `parallelize` function on your Clojure data structure:

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]))

(def data (f/parallelize sc [["a" 1] ["b" 2] ["c" 3] ["d" 4] ["e" 5]]))
```

Once initialized, the distributed dataset or RDD can be operated on in parallel.

An important parameter for parallel collections is the number of slices to cut the dataset into. Spark runs one task for each slice of the cluster. Normally, Spark tries to set the number of slices automatically based on your cluster. However, you can also set it manually in flambo by passing it as a third parameter to parallelize:

```clojure
(def data (f/parallelize sc [1 2 3 4 5] 4))
```

#### External Datasets

Spark can create RDDs from any storage source supported by Hadoop, including the local file system, HDFS, Cassandra, HBase, Amazon S3, etc. Spark supports text files, SequenceFiles, and any other Hadoop InputFormat.

Text file RDDs can be created in flambo using the `text-file` function under the `flambo.api` namespace. This function takes a URI for the file (either a local path on the machine, or a `hdfs://...`, `s3n://...`, etc URI) and reads it as a collection of lines. Note, `text-file` supports S3 and HDFS globs.

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]))

(def data (f/text-file sc "hdfs://hostname:<port>/home/user/data_archive/2013/12/23/*/*.bz2"))
```

<a name="rdd-operations">
### RDD Operations

RDDs support two types of operations:

* [_transformations_](#rdd-transformations), which create a new dataset from an existing one
* [_actions_](#rdd-actions), which return a value to the driver program after running a computation on the dataset

<a name="basics">
#### Basics

To illustrate RDD basics in flambo, consider the following simple application using the sample `data.txt` file located at the root of the flambo repo.

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]))

;; NOTE: we are using the flambo.api/fn not clojure.core/fn
(-> (f/text-file sc "data.txt")   ;; returns an unrealized lazy dataset
    (f/map (f/fn [s] (count s)))  ;; returns RDD array of length of lines
    (f/reduce (f/fn [x y] (+ x y)))) ;; returns a value, should be 1406
```

The first line defines a base RDD from an external file. The dataset is not loaded into memory; it is merely a pointer to the file. The second line defines an RDD of the lengths of the lines as a result of the `map` transformation. Note, the lengths are not immediately computed due to laziness. Finally, we run `reduce` on the transformed RDD, which is an action, returning only a _value_ to the driver program.

If we also wanted to reuse the resulting RDD of length of lines in later steps, we could insert:

```clojure
(f/cache)
```

before the `reduce` action, which would cause the line-lengths RDD to be saved to memory after the first time it is realized. See [RDD Persistence](#rdd-persistence) for more on persisting and caching RDDs in flambo.

<a name="flambo-functions">
#### Passing Functions to flambo

Spark’s API relies heavily on passing functions in the driver program to run on the cluster. Flambo makes it easy and natural to define serializable Spark functions/operations and provides two ways to do this:

* `flambo.api/defsparkfn`: defines named functions:

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]))

(f/defsparkfn square [x] (* x x))
```

* `flambo.api/fn`: defines inline anonymous functions:

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]))

(-> (f/parallelize sc [1 2 3 4 5])
    (f/map (f/fn [x] (* x x))))
```

When we evaluate this `map` transformation on the initial RDD, the result is another RDD. The result of this transformation can be seen using the `f/collect` action to return all of the elements of the RDD.

```clojure
(-> (f/parallelize sc [1 2 3 4 5])
    (f/map (f/fn [x] (* x x)))
    f/collect)
;; => [1 4 9 16 25]
```
We can also use `f/first` or `f/take` to return just a subset of the data.

```clojure
(-> (f/parallelize sc [1 2 3 4 5])
    (f/map square)
    (f/take 2))
;; => [1 4]
```

<a name="key-value-pairs">
#### Working with Key-Value Pairs

While most Spark operations work on RDDs containing any type of objects, a few special operations are only available on RDDs of key-value pairs. The most common ones are distributed "shuffle" operations, such as grouping or aggregating the elements by a key.

In flambo, these operations are available on RDDs of (key, value) tuples. Flambo handles all of the transformations/serializations to/from `Tuple`, `Tuple2`, `JavaRDD`, `JavaPairRDD`, etc., so you only need to define the sequence of operations you'd like to perform on your data.

The following code uses the `reduce-by-key` operation on key-value pairs to count how many times each word occurs in a file:

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]
            [clojure.string :as s]))

(-> (f/text-file sc "data.txt")
    (f/flat-map (f/fn [l] (s/split l #" ")))
    (f/map (f/fn [w] [w 1]))
    (f/reduce-by-key (f/fn [x y] (+ x y))))
```

After the `reduce-by-key` operation, we can sort the pairs alphabetically using `f/sort-by-key`. To collect the word counts as an array of objects in the repl or to write them to a filesysten, we can use the `f/collect` action:

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]
            [clojure.string :as s]))

(-> (f/text-file sc "data.txt")
    (f/flat-map (f/fn [l] (s/split l #" ")))
    (f/map (f/fn [w] [w 1]))
    (f/reduce-by-key (f/fn [x y] (+ x y)))
    f/sort-by-key
    f/collect
    clojure.pprint/pprint)
```

<a name="rdd-transformations">
#### RDD Transformations

Flambo supports the following RDD transformations:

* `map`: returns a new RDD formed by passing each element of the source through a function.
* `map-to-pair`: returns a new `JavaPairRDD` of (K, V) pairs by applying a function to all elements of an RDD.
* `reduce-by-key`: when called on an RDD of (K, V) pairs, returns an RDD of (K, V) pairs where the values for each key are aggregated using a reduce function.
* `flat-map`: similar to `map`, but each input item can be mapped to 0 or more output items (so the function should return a collection rather than a single item)
* `filter`: returns a new RDD containing only the elements of the source RDD that satisfy a predicate function.
* `join`: when called on an RDD of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key.
* `left-outer-join`: performs a left outer join of a pair of RDDs. For each element (K, V) in the first RDD, the resulting RDD will either contain all pairs (K, (V, W)) for W in second RDD, or the pair (K, (V, nil)) if no elements in the second RDD have key K.
* `sample`: returns a 'fraction' sample of an RDD, with or without replacement, using a random number generator 'seed'.
* `combine-by-key`: combines the elements for each key using a custom set of aggregation functions. Turns an RDD of (K, V) pairs into a result of type (K, C), for a 'combined type' C. Note that V and C can be different -- for example, one might group an RDD of type (Int, Int) into an RDD of type (Int, List[Int]). Users must provide three functions:
    - createCombiner, which turns a V into a C (e.g., creates a one-element list)
    - mergeValue, to merge a V into a C (e.g., adds it to the end of a list)
    - mergeCombiners, to combine two C's into a single one.
* `sort-by-key`: when called on an RDD of (K, V) pairs where K implements ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified by the optional boolean ascending argument.
* `coalesce`: decreases the number of partitions in an RDD to 'n'. Useful for running operations more efficiently after filtering down a large dataset.
* `group-by`: returns an RDD of items grouped by the return value of a function.
* `group-by-key`: groups the values for each key in an RDD into a single sequence.
* `flat-map-to-pair`: returns a new `JavaPairRDD` by first applying a function to all elements of the RDD, and then flattening the results.

<a name="rdd-actions">
#### RDD Actions

Flambo supports the following RDD actions:

* `reduce`: aggregates the elements of an RDD using a function which takes two arguments and returns one. The function should be commutative and associative so that it can be computed correctly in parallel.
* `count-by-key`: only available on RDDs of type (K, V). Returns a map of (K, Int) pairs with the count of each key.
* `foreach`: applies a function to all elements of an RDD.
* `fold`: aggregates the elements of each partition, and then the results for all the partitions using an associative function and a neutral 'zero value'.
* `first`: returns the first element of an RDD.
* `count`: returns the number of elements in an RDD.
* `collect`: returns all the elements of an RDD as an array at the driver process.
* `distinct`: returns a new RDD that contains the distinct elements of the source RDD.
* `take`: returns an array with the first n elements of the RDD.
* `glom`: returns an RDD created by coalescing all elements of the source RDD within each partition into a list.
* `cache`: persists an RDD with the default storage level ('MEMORY_ONLY').

<a name="rdd-persistence">
### RDD Persistence

Spark provides the ability to persist (or cache) a dataset in memory across operations. Spark’s cache is fault-tolerant – if any partition of an RDD is lost, it will automatically be recomputed using the transformations that originally created it. Caching is a key tool for iterative algorithms and fast interactive use. Like Spark, flambo provides the functions `f/persist` and `f/cache` to persist RDDs. `f/persist` sets the storage level of an RDD to persist its values across operations after the first time it is computed. Storage levels are available in the `flambo.api/STORAGE-LEVELS` map. This can only be used to assign a new storage level if the RDD does not have a storage level set already. `cache` is a convenience function for using the default storage level, 'MEMORY_ONLY'.

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]))

(let [line-lengths (-> (f/text-file sc "data.txt")
                       (f/map (f/fn [s] (count s)))
                       f/cache)]
  (-> line-lengths
      (f/reduce (f/fn [x y] (+ x y)))))
```

<a name="running-flambo">
### Standalone Applications

To run your flambo application as a standalone application using the Spark API, you'll need to package your application in an uberjar using `lein` and execute it with:

* `SPARK_CLASSPATH`, if running Spark 0.9.1
* `./bin/spark-submit`, if running Spark 1.0.0 or greater

```shell
$ lein uberjar
...

$ SPARK_CLASSPATH=uberjar.jar spark-class com.some.class.with.main --flag1 arg1 --flag2 arg2
...
<output>

$ spark-submit --class com.some.class.with.main uberjar.jar --flag1 arg1 --flag2 arg2
...
<output>
```

<a name="kryo">
## Kryo

Flambo requires that Spark is configured to use kryo for serialization. This is configured by default using system properties.

If you need to register custom serializers, extend `flambo.kryo.BaseFlamboRegistrator` and override its `register` method. Finally, configure your SparkContext to use your custom registrator by setting `spark.kryo.registrator` to your custom class.

There is a convenience macro for creating registrators, `flambo.kryo.defregistrator`. The namespace where a registrator is defined should be AOT compiled.

Here is an Example (this won't work in your REPL):

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.kryo :as kryo])
  (:import [flameprincess FlamePrincessHeat FlamePrincessHeatSerializer]))

(kryo/defregistrator flameprincess [kryo]
  (.register kryo FlamePrincessHeat (FlamePrincessHeatSerializer.)))

(def c (-> (conf/spark-conf)
       (conf/set "spark.kryo.registrator" flameprincess)))
```

<a name="acknowledgements">
## Acknowledgements

Thanks to The Climate Corporation and their open source project [clj-spark](https://github.com/TheClimateCorporation/clj-spark) which served as the starting point for this project.

Thanks to [Ben Black](https://github.com/b) for doing the work on the streaming api.


## License

Copyright © 2014 Yieldbot, Inc.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
