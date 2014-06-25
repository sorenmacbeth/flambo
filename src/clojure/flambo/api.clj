(ns flambo.api
  (:refer-clojure :exclude [fn map reduce first count take distinct filter group-by])
  (:require [serializable.fn :as sfn]
            [clojure.tools.logging :as log]
            [flambo.function :refer [flat-map-function
                                     flat-map-function2
                                     function
                                     function2
                                     function3
                                     pair-function
                                     pair-flat-map-function
                                     void-function]]
            [flambo.conf :as conf]
            [flambo.utils :as u]
            [flambo.kryo :as k])
  (:import (scala Tuple2)
           [scala.reflect ClassTag$]
           (java.util Comparator)
           (org.apache.spark.api.java JavaSparkContext StorageLevels)
           org.apache.spark.api.java.JavaRDD
           (org.apache.spark.rdd PartitionwiseSampledRDD)
           (flambo.function Function Function2 Function3 VoidFunction FlatMapFunction
                            PairFunction PairFlatMapFunction)))

(System/setProperty "spark.serializer" "org.apache.spark.serializer.KryoSerializer")
(System/setProperty "spark.kryo.registrator" "flambo.kryo.BaseFlamboRegistrator")

(def STORAGE-LEVELS {:memory-only StorageLevels/MEMORY_ONLY
                     :memory-only-ser StorageLevels/MEMORY_ONLY_SER
                     :memory-and-disk StorageLevels/MEMORY_AND_DISK
                     :memory-and-disk-ser StorageLevels/MEMORY_AND_DISK_SER
                     :disk-only StorageLevels/DISK_ONLY
                     :memory-only-2 StorageLevels/MEMORY_ONLY_2
                     :memory-only-ser-2 StorageLevels/MEMORY_ONLY_SER_2
                     :memory-and-disk-2 StorageLevels/MEMORY_AND_DISK_2
                     :memory-and-disk-ser-2 StorageLevels/MEMORY_AND_DISK_SER_2
                     :disk-only-2 StorageLevels/DISK_ONLY_2})

(defmacro fn
  [& body]
  `(sfn/fn ~@body))

(defmacro defsparkfn
  [name & body]
  `(def ~name
     (fn ~@body)))

(defn spark-context
  "Creates a spark context that loads settings from given configuration object
   or system properties"
  ([conf]
     (log/debug "JavaSparkContext" (conf/to-string conf))
     (JavaSparkContext. conf))
  ([master app-name]
     (log/debug "JavaSparkContext" master app-name)
     (JavaSparkContext. master app-name)))

(defn local-spark-context
  [app-name]
  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name app-name))]
    (spark-context conf)))

(defmacro with-context
  [context-sym conf & body]
  `(let [~context-sym (f/spark-context ~conf)]
     (try
       ~@body
       (finally (.stop ~context-sym)))))

(defn jar-of-ns
  [ns]
  (let [clazz (Class/forName (clojure.string/replace (str ns) #"-" "_"))]
    (JavaSparkContext/jarOfClass clazz)))

(defsparkfn untuple [^Tuple2 t]
  (let [v (transient [])]
    (conj! v (._1 t))
    (conj! v (._2 t))
    (persistent! v)))

(defsparkfn double-untuple [^Tuple2 t]
  (let [[x ^Tuple2 t2] (untuple t)
        v (transient [])]
    (conj! v x)
    (conj! v (untuple t2))
    (persistent! v)))

(defsparkfn group-untuple [^Tuple2 t]
  (let [v (transient [])]
    (conj! v (._1 t))
    (conj! v (into [] (._2 t)))
    (persistent! v)))

(defn- ftruthy?
  [f]
  (fn [x] (u/truthy? (f x))))

;; ## RDD construction
;;
;; Function for constructing new RDDs
;;
(defn text-file [spark-context filename]
  "Reads a text file from HDFS, a local file system (available on all nodes),
  or any Hadoop-supported file system URI, and returns it as an RDD of Strings"
  (.textFile spark-context filename))

(defn parallelize
  "Distributes a local collection to form/return an RDD"
  ([spark-context lst] (.parallelize spark-context lst))
  ([spark-context lst num-slices] (.parallelize spark-context lst num-slices)))

(defn partitionwise-sampled-rdd [rdd sampler seed]
  "Creates a PartitionwiseSampledRRD from existing RDD and a sampler object"
  (-> (PartitionwiseSampledRDD. (.rdd rdd) sampler seed k/OBJECT-CLASS-TAG k/OBJECT-CLASS-TAG)
      (JavaRDD/fromRDD k/OBJECT-CLASS-TAG)))

;; ## Transformations
;;
;; Function for transforming RDDs
;;
(defn map
  "Returns a new RDD formed by passing each element of the source through the function f"
  [rdd f]
  (.map rdd (function f)))

(defn map-to-pair
  "Returns a new RDD of (K, V) pairs by applying f to all elements of this RDD"
  [rdd f]
  (.mapToPair rdd (pair-function f)))

(defn reduce
  "Aggregates the elements of the dataset using the function f (which takes two arguments
  and returns one). The function should be commutative and associative so that it can be
  computed correctly in parallel"
  [rdd f]
  (.reduce rdd (function2 f)))

(defn flat-map
  "Similar to map, but each input item can be mapped to 0 or more output items (so the
   fucntion f should return a Seq rather than a single item)"
  [rdd f]
  (.flatMap rdd (flat-map-function f)))

(defn flat-map-to-pair
  "Returns a new RDD by first applying f to all elements of this RDD, and then flattening
  the results"
  [rdd f]
  (.flatMapToPair rdd (pair-flat-map-function f)))

(defn filter
  "Returns a new RDD containing only the elements that satisfy a predicate"
  [rdd f]
  (.filter rdd (function (ftruthy? f))))

(defn foreach
  "Applies the function f to all elements of this RDD"
  [rdd f]
  (.foreach rdd (void-function f)))

(defn aggregate
  "Aggregates the elements of each partition, and then the results for all the partitions,
   using a given combine function and a neutral 'zero value'"
  [rdd zero-value seq-op comb-op]
  (.aggregate rdd zero-value (function2 seq-op) (function2 comb-op)))

(defn fold
  "Aggregates the elements of each partition, and then the results for all the partitions,
  using a given associative function and a neutral 'zero value'"
  [rdd zero-value f]
  (.fold rdd zero-value (function2 f)))

(defn reduce-by-key
  "When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs
  where the values for each key are aggregated using the given reduce function f"
  [rdd f]
  (-> rdd
      (map-to-pair identity)
      (.reduceByKey (function2 f))
      (.map (function untuple))))

(defn group-by
  "Returns an RDD of grouped items"
  [rdd f]
  (-> rdd
      (.groupBy (function f))
      (.map (function group-untuple))))

(defn group-by-key
  "Groups the values for each key in the RDD into a single sequence"
  [rdd]
  (-> rdd
      (map-to-pair identity)
      .groupByKey
      (.map (function group-untuple))))

(defn combine-by-key
  "Combines the elements for each key using a custom set of aggregation functions.
  Turns an RDD of (K, V) pairs into a result of type (K, C), for a 'combined type' C.
  Note that V and C can be different -- for example, one might group an RDD of type
  (Int, Int) into an RDD of type (Int, List[Int]).
  Users must provide three functions:
  -- createCombiner, which turns a V into a C (e.g., creates a one-element list)
  -- mergeValue, to merge a V into a C (e.g., adds it to the end of a list)
  -- mergeCombiners, to combine two C's into a single one."
  [rdd create-combiner merge-value merge-combiners]
  (-> rdd
      (map-to-pair identity)
      (.combineByKey (function create-combiner)
                     (function2 merge-value)
                     (function2 merge-combiners))
      (.map (function untuple))))

(defn sort-by-key
  "When called on a dataset of (K, V) pairs where K implements ordered, returns a dataset of
   (K, V) pairs sorted by keys in ascending or descending order, as specified by the boolean
   ascending argument"
  ([rdd]
     (sort-by-key rdd compare true))
  ([rdd x]
     ;; RDD has a .sortByKey signature with just a Boolean arg, but it doesn't
     ;; seem to work when I try it, bool is ignored.
     (if (instance? Boolean x)
       (sort-by-key rdd compare x)
       (sort-by-key rdd x true)))
  ([rdd compare-fn asc?]
     (-> rdd
         (map-to-pair identity)
         (.sortByKey
          (if (instance? Comparator compare-fn)
            compare-fn
            (comparator compare-fn))
          (u/truthy? asc?))
         (.map (function untuple)))))

(defn join
  "When called on datasets of type (K, V) and (K, W), returns a dataset of
  (K, (V, W)) pairs with all pairs of elements for each key"
  [rdd other]
  (-> rdd
      (map-to-pair identity)
      (.join (map-to-pair other identity))
      (.map (function double-untuple))))

(defn left-outer-join
  "Performs a left outer join of the given RDD and other. For each element (K, V)
   in the RDD, the resulting RDD will either contain all pairs (K, (V, W)) for W in other,
   or the pair (K, (V, nil)) if no elements in other have key K"
  [rdd other]
  (-> rdd
      (map-to-pair identity)
      (.leftOuterJoin (map-to-pair other identity))
      (.map (function
             (fn [t]
                      (let [[x t2] (untuple t)
                            [a b] (untuple t2)]
                        (vector x [a (.orNull b)])))))))

(defn sample
  "Returns a fraction sample of the data, with or without replacement,
  using a given random number generator seed"
  [rdd with-replacement? fraction seed]
  (.sample rdd with-replacement? fraction seed))

;; ## Actions
;;
;; Action return their results to the driver process.
;;
(defn count-by-key
  "Only available on RDDs of type (K, V).
  Returns a map of (K, Int) pairs with the count of each key"
  [rdd]
  (into {}
        (-> rdd
            (map-to-pair identity)
            .countByKey)))

(defn save-as-text-file
  "Writes the elements of the RDD as a text file (or set of text files)
  in a given directory in the local filesystem, HDFS or any other Hadoop-supported
  file system. Spark will call toString on each element to convert it to a line of
  text in the file"
  [rdd path]
  (.saveAsTextFile rdd path))

(defn save-as-sequence-file
  "Writes the elements of the dataset as a Hadoop SequenceFile in a given path
   in the local filesystem, HDFS or any other Hadoop-supported file system.
   This is available on RDDs of key-value pairs that either implement Hadoop's
   Writable interface"
  [rdd path]
  (.saveAsSequenceFile rdd path))

(defn persist
  "Sets this RDD's storage level to persist its values across operations
  after the first time it is computed. This can only be used to assign a
  new storage level if the RDD does not have a storage level set already"
  [rdd storage-level]
  (.persist rdd storage-level))

(def first
  "Returns the first element of the RDD"
  (memfn first))

(def count
  "Return the number of elements in the RDD"
  (memfn count))

(def glom
  "Returns an RDD created by coalescing all elements within each partition into a list"
  (memfn glom))

(def cache
  "Persists this RDD with the default storage level (`MEMORY_ONLY`)"
  (memfn cache))

(def collect
  "Returns all the elements of the dataset as an array at the driver program"
  (memfn collect))

(def distinct
  "Return a new RDD that contains the distinct elements of the source RDD"
  (memfn distinct))

(defn take
  "Return an array with the first n elements of the RDD.
  (Note: this is currently not executed in parallel. Instead, the driver
  program computes all the elements)"
  [rdd cnt]
  (.take rdd cnt))

(defn coalesce
  "Decrease the number of partitions in the RDD to n.
  Useful for running operations more efficiently after filtering down a large dataset"
  [rdd n]
  (.coalesce rdd n))
