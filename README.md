![Flambo](http://static1.wikia.nocookie.net/__cb20120216165717/adventuretimewithfinnandjake/images/e/ee/Flambos_fire_magic.jpg)

# flambo

Flambo is a Clojure DSL for [Apache Spark](http://spark.apache.org/docs/latest/)

**Contents**

* [Overview](#overview)
* [Supported Spark Versions](#versions)
* [Installation](#installation)
* [Usage](#usage)
* [Kryo](#kryo)
* [Terminology](#terminology)

<a name="overview">
## Overview

Apache Spark is a fast and general-purpose cluster computing system. It provides high-level APIs in Java, Scala and Python, and an optimized engine that supports general execution graphs.

Flambo is Clojure a DSL that allows you access all of the APIs that Spark has to offer. It provides for a more Clojure like view of the Spark data structures; untyped, letting functions deal with data of various shapes. With flambo, you directly work with Clojure data structures. Flambo is pure Clojure. Who knows, it might even convince you to move to Clojure (crossing fiery, flaming fingers).

"So that's where I came from." --flambo

<a name="versions">
## Supported Spark Versions

Flambo 0.3.0-SNAPSHOT **requires** Spark 1.0.0
 
Flambo 0.2.0 targets Spark 0.9.1

<a name="installation">
## Installation

Flambo is available from clojars. Depending on the version of Spark your developing with, please add the following to your project.clj's `deps`:

### With Leiningen

`[yieldbot/flambo "0.3.0-SNAPSHOT"]` for Spark 1.0.0

`[yieldbot/flambo "0.2.0"]` for Spark 0.9.1

<a name="usage">
## Usage

Flambo makes developing Spark applications in Clojure as quick and painless as possible, with the added benefit of using the full abstraction of Clojure. For instance, as we'll see, you can use the standard Clojure threading macros `->` (ftw) to define/chain your sequence of operations and transformations.

<a name="initializing-flambo">
### Initializing flambo

The first thing a Spark program must do is to create a Spark context object, which tells Spark how to access a cluster. To create a Spark context in flambo you first need to build a Spark configuration object that contains information about your application.

Here we create a Spark configuration object with the special `local[*]` string to run in local mode:

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.conf :as conf])
  (:require [flambo.api :as f]))
  
(def c (-> (conf/spark-conf) 
           (conf/master "local[*]") 
           (conf/app-name "flambo")))
           
(def sc (f/spark-context c))
```

The `master` url string can be `spark://...`, `mesos://...`, yarn-cluster or `local`. 

However, in practice, when running on a cluster, you will not want to hardcode the `master` setting, instead, launch the application with `spark-submit` and receive it there. For local testing and unit tests, you can pass `local` or `local[*]` to run Spark in-process.

The `app-name` configuration setting is the name for your application to show on the Mesos cluster UI.

<a name="rdds">
### Resilient Distributed Datasets (RDDs)

The main abstraction Spark provides is a _resilient distributed dataset_ ([RDD](#terminology)), which is a fault-tolerant collection of elements partitioned across the nodes of the cluster that can be operated on in parallel. There are two ways to create RDDs: _parallelizing_ an existing collection in your driver program, or referencing a dataset in an external storage system, such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.

#### Parallelized Collections

Parallelized collections (RDDs) in flambo are created by calling the `parallelize` function on your Clojure data structure:

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]))

(def data (f/parallelize sc [["a" 1] ["b" 2] ["c" 3] ["d" 4] ["e" 5]]))
```

Once initialized, the distributed dataset or RDD can be operated on in parallel, as we will see shortly.

An important parameter for parallel collections is the number of slices to cut the dataset into. Spark runs one task for each slice of the cluster. Normally, Spark tries to set the number of slices automatically based on your cluster. However, you can also set it manually in flambo by passing it as a second parameter to parallelize:

```clojure
(def data (f/parallelize sc [1 2 3 4 5] 4))
```

#### External Datasets

Spark can create RDDs from any storage source supported by Hadoop, including your local file system, HDFS, Cassandra, HBase, Amazon S3, etc. Spark supports text files, SequenceFiles, and any other Hadoop InputFormat.

Text file RDDs can be created in flambo using the `text-file` function under the `flambo.api` namespace. This function takes a URI for the file (either a local path on the machine, or a `hdfs://...`, `s3n://...`, etc URI) and reads it as a collection of lines. Note, `text-file` supports S3, HDFS globs.

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]))
  
(def data (f/text-file sc "hdfs://hostname:<port>/home/user/data_archive/2013/12/23/*/*.bz2"))
```

<a name="rdd-operations">
### RDD Operations

RDDs support two types of operations: [_transformations_](#rdd-transformations), which create a new dataset from an existing one, and [_actions_](#rdd-actions), which return a value to the driver program after running a computation on the dataset.

<a name="basics">
#### Basics

To illustrate RDD basics in flambo, consider the following simple application:

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]))

;; NOTE: we are using the flambo.api/fn not clojure.core/fn
(-> (f/text-file sc "data.txt")  ;; returns an unrealized lazy dataset
    (f/map (f/fn [s] (count s)))  ;; returns RDD array of length of lines
    (f/reduce (f/fn [x y] (+ x y))))  ;; returns a value
```

The first line defines a base RDD from an external file. The dataset is not loaded into memory or otherwise acted on, it is merely a pointer to the file. The second line defines an RDD of the lengths of the lines as a result of the `map` transformation; note, the lengths are not immediately computed, due to laziness. Finally, we run `reduce` on the transformed RDD, which is an action, returning only a value to the driver program.

If we also wanted to reuse the resulting RDD of length of lines in later step, we could add:

```clojure
(f/cache)
```

before the `reduce` action, which would cause line lengths RDD to be save in memory after the first time it is realized. More on persisting and caching RDDs in memory later.

<a name="spark-functions">
#### Passing Functions to Spark

Spark’s API relies heavily on passing functions in the driver program to run on the cluster. Flambo makes it is easy and natural to define serializable spark functions/operations and provides two ways to do this:

* `flambo.api/defsparkfn`: which are just normal functions, making it easier to write test against operations: 

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]))
  
(f/defsparkfn square [x] (* x x))
```

* `flambo.api/fn`: which defines an inline anonymous function: 

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]))
  
(-> (f/parallelize sc [1 2 3 4 5]) 
    (f/map (f/fn [x] (* x x))))
```

<a name="key-value-pairs">
#### Working with Key-Value Pairs

While most Spark operations work on RDDs containing any type of objects, a few special operations are only available on RDDs of key-value pairs. The most common ones are distibuted “shuffle” operations, such as grouping or aggregating the elements by a key.

In flambo, these operations are available on RDDs of (key, value) tuples. However, flambo's superawesome macros transparently handle all transformations/serializations to/from Tuple, Tuple2, JavaRDD, JavaPairRDD, etc, class type ickiness. You, dear users, are shielded from the fiery bowls of serialization hell, and only need to define the sequence of operations you'd like to perform on your dataset(s) to enjoy the cool breeze of zenness...

For example, the following code uses the `reduce-by-key` operation on key-value pairs to count how many times each word occurs in a file:

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]
            [clojure.string :as s]))

(-> (f/text-file sc "data.txt")
    (f/flat-map (f/fn [l] (s/split l #" ")))
    (f/map (f/fn [w] [w 1]))
    (f/reduce-by-key (f/fn [x y] (+ x y)))
    f/collect)
```

We could also use `f/sort-by-key`, for example, to sort the pairs alphabetically, and finally `f/collect` to bring them back to the driver program as an array of objects.

<a name="rdd-transformations">
#### RDD Transformations

Flambo supports the following RDD transformations:

* `map`: returns a new RDD formed by passing each element of the source through the function `f`.
* `map-to-pair`: returns a new `JavaPairRDD` of (K, V) pairs by applying `f` to all elements of an RDD.
* `reduce-by-key`: when called on an RDD of (K, V) pairs, returns an RDD of (K, V) pairs where the values for each key are aggregated using the given reduce function `f`.
* `flat-map`: similar to `map`, but each input item can be mapped to 0 or more output items (so the function `f` should return a collection rather than a single item)
* `filter`: returns a new RDD containing only the elements of the RDD that satisfy a predicate `f`.
* `join`: When called on an RDD of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key.
* `left-outer-join`: Performs a left outer join of _rdd_ and _other_. For each element (K, V) in the RDD, the resulting RDD will either contain all pairs (K, (V, W)) for W in other, or the pair (K, (V, nil)) if no elements in other have key K.
* `sample`: returns a 'fraction' sample of an RDD, with or without replacement, using a given random number generator 'seed'.
* `combine-by-key`: combines the elements for each key using a custom set of aggregation functions. Turns an RDD of (K, V) pairs into a result of type (K, C), for a 'combined type' C. Note that V and C can be different -- for example, one might group an RDD of type (Int, Int) into an RDD of type (Int, List[Int]).
          Users must provide three functions:
          -- createCombiner, which turns a V into a C (e.g., creates a one-element list)
          -- mergeValue, to merge a V into a C (e.g., adds it to the end of a list)
          -- mergeCombiners, to combine two C's into a single one.
* `sort-by-key`: when called on an RDD of (K, V) pairs where K implements ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified by the boolean ascending argument.
* `coalesce`: decreases the number of partitions in an RDD to 'n'. Useful for running operations more efficiently after filtering down a large dataset.
* `group-by`: returns an RDD of items grouped by the return value of function `f`.
* `group-by-key`: groups the values for each key in an RDD into a single sequence.
* `flat-map-to-pair`: returns a new `JavaPairRDD` by first applying `f` to all elements of the RDD, and then flattening the results.

<a name="rdd-actions">
#### RDD Actions

Flambo supports the following RDD actions:

* `reduce`: aggregates the elements of an RDD using the function `f` (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel.
* `count-by-key`: only available on RDDs of type (K, V). Returns a map of (K, Int) pairs with the count of each key.
* `foreach`: applies the given function `f` to all elements of an RDD.
* `fold`: aggregates the elements of each partition, and then the results for all the partitions, using a given associative function and a neutral 'zero value'.
* `first`: returns the first element of an RDD.
* `count`: returns the number of elements in an RDD.
* `collect`: returns all the elements of an RDD as an array at the driver process.
* `distinct`: returns a new RDD that contains the distinct elements of the source RDD.
* `take`: returns an array with the first n elements of the RDD.
* `glom`: returns an RDD created by coalescing all elements of `rdd` within each partition into a list.
* `cache`: persists an RDD with the default storage level ('MEMORY_ONLY').

<a name="rdd-persistence">
### RDD Persistence

Spark provides the ability to persist (or cache) a dataset in memory across operations. Spark’s cache is fault-tolerant – if any partition of an RDD is lost, it will automatically be recomputed using the transformations that originally created it. Caching is a key tool for iterative algorithms and fast interactive use, so flambo provides the functions `f/persist` and `f/cache` to mark an RDD to be persisted. `f/persist` sets the storage level of an RDD to persist its values across operations after the first time it is computed. Storage levels are available in the `flambo.api/STORAGE-LEVELS` map. This can only be used to assign a new storage level if the RDD does not have a storage level set already. `cache` is a convenience function for using the default storage level, 'MEMORY_ONLY'.

```clojure
(ns com.fire.kingdom.flambit
  (:require [flambo.api :as f]))

(let [line-lengths (-> (f/text-file sc "data.txt")
                       (f/map (f/fn [s] (count s)))
                       f/cache)]
  (-> line-lengths
      (f/reduce (f/fn [x y] (+ x y)))))
```

<a name="kryo">
## Kryo

Flambo requires that spark is configured to use kryo for serialization. This is configured by default using system properties.

If you need to register custom serializers, extend `flambo.kryo.BaseFlamboRegistrator` and override it's `register` method. Finally, configure your SparkContext to use your custom registrator by setting `spark.kryo.registrator` to your custom class.

There is a convenience macro for creating registrators, `flambo.kryo.defregistrator`. The namespace where a registrator is defined should be AOT compiled.

In a REPL:

```clojure
(require '[flambo.kryo :as kryo])
(import '[flameprincess FlamePrincessHeat FlamePrincessHeatSerializer])

(kryo/defregistrator flameprincess [kryo]
(.register kryo FlamePrincessHeat (FlamePrincessHeatSerializer.)))

(def c (-> (conf/spark-conf) (conf/set "spark.kryo.registrator" "my.namespace.registrator.flameprincess")
```

<a name="terminology">
## Terminology

* **RDD**: A resilient distributed dataset, which is a fault-tolerant collection of elements that can be operated on in parallel.


## License

Copyright © 2014 Soren Macbeth

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
