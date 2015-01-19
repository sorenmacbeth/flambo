(ns flambo.api-test
  (:use midje.sweet)
  (:require [flambo.api :as f]
            [flambo.conf :as conf]))

(facts
 "about spark-context"
 (let [conf (-> (conf/spark-conf)
                (conf/master "local[*]")
                (conf/app-name "api-test"))]
   (f/with-context c conf
     (fact
      "gives us a JavaSparkContext"
      (class c) => org.apache.spark.api.java.JavaSparkContext)

     (fact
      "creates a JavaRDD"
      (class (f/parallelize c [1 2 3 4 5])) => org.apache.spark.api.java.JavaRDD)

     (fact
      "round-trips a clojure vector"
      (-> (f/parallelize c [1 2 3 4 5]) f/collect vec) => (just [1 2 3 4 5]))

     (fact
      "union concats two RDDs"
      (let [rdd1 (f/parallelize c [1 2 3 4])
            rdd2 (f/parallelize c [11 12 13])
            rdd3 (f/parallelize c [21 22 23])]
        (-> (f/union c rdd1 rdd2 rdd3)
            f/collect
            vec) => (just [1 2 3 4 11 12 13 21 22 23] :in-any-order))))))

(facts
 "about serializable functions"

 (let [myfn (f/fn [x] (* 2 x))]
   (fact
    "inline op returns a serializable fn"
    (type myfn) => :serializable.fn/serializable-fn)

   (fact
    "we can serialize it to a byte-array"
    (class (serializable.fn/serialize myfn)) => (Class/forName "[B"))

   (fact
    "it round-trips back to a serializable fn"
    (type (-> myfn serializable.fn/serialize serializable.fn/deserialize)) => :serializable.fn/serializable-fn)))

(facts
 "about untupling"

 (fact
  "untuple returns a 2 vector"
  (let [tuple2 (scala.Tuple2. 1 "hi")]
    (f/untuple tuple2) => [1 "hi"]))

 (fact
  "double untuple returns a vector with a key and a 2 vector value"
  (let [double-tuple2 (scala.Tuple2. 1 (scala.Tuple2. 2 "hi"))]
    (f/double-untuple double-tuple2) => [1 [2 "hi"]])))

(facts
  "about transformations"

  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name "api-test"))]
    (f/with-context c conf
      (fact
        "map returns an RDD formed by passing each element of the source RDD through a function"
        (-> (f/parallelize c [1 2 3 4 5])
            (f/map (f/fn [x] (* 2 x)))
            f/collect
            vec) => [2 4 6 8 10])

      (fact
        "map-to-pair returns an RDD of (K, V) pairs formed by passing each element of the source
        RDD through a pair function"
        (-> (f/parallelize c ["a" "b" "c" "d"])
            (f/map-to-pair (f/fn [x] [x 1]))
            (f/map f/untuple)
            f/collect
            vec) => [["a" 1] ["b" 1] ["c" 1] ["d" 1]])

      (fact
        "reduce-by-key returns an RDD of (K, V) when called on an RDD of (K, V) pairs"
        (-> (f/parallelize c [["key1" 1]
                              ["key1" 2]
                              ["key2" 3]
                              ["key2" 4]
                              ["key3" 5]])
            (f/reduce-by-key (f/fn [x y] (+ x y)))
            f/collect
            vec) => (contains #{["key1" 3] ["key2" 7] ["key3" 5]}))

      (fact
        "similar to map, but each input item can be mapped to 0 or more output items;
        mapping function must therefore return a sequence rather than a single item"
        (-> (f/parallelize c [["Four score and seven years ago our fathers"]
                              ["brought forth on this continent a new nation"]])
            (f/flat-map (f/fn [x] (clojure.string/split (first x) #" ")))
            f/collect
            vec) => ["Four" "score" "and" "seven" "years" "ago" "our" "fathers" "brought" "forth" "on" "this" "continent" "a" "new" "nation"])

      (fact
        "filter returns an RDD formed by selecting those elements of the source on which func returns true"
        (-> (f/parallelize c [1 2 3 4 5 6])
            (f/filter (f/fn [x] (even? x)))
            f/collect
            vec) => [2 4 6])

      (fact
        "join returns an RDD of (K, (V, W)) pairs with all pairs of elements of each key when called on RDDs of type (K, V) and (K, W)"
        (let [LDATA (f/parallelize c [["key1" [2]]
                                      ["key2" [3]]
                                      ["key3" [5]]
                                      ["key4" [1]]
                                      ["key5" [2]]])
              RDATA (f/parallelize c [["key1" [22]]
                                      ["key3" [33]]
                                      ["key4" [44]]])
              ]
          (-> (f/join LDATA RDATA)
              f/collect
              vec)) => (just [["key3" [[5] [33]]]
                              ["key4" [[1] [44]]]
                              ["key1" [[2] [22]]]] :in-any-order))

      (fact
        "left-outer-join returns an RDD of (K, (V, W)) when called on RDDs of type (K, V) and (K, W)"
        (let [LDATA (f/parallelize c [["key1" [2]]
                                      ["key2" [3]]
                                      ["key3" [5]]
                                      ["key4" [1]]
                                      ["key5" [2]]])
              RDATA (f/parallelize c [["key1" [22]]
                                      ["key3" [33]]
                                      ["key4" [44]]])]
          (-> (f/left-outer-join LDATA RDATA)
              f/collect
              vec)) => (just [["key3" [[5] [33]]]
                              ["key4" [[1] [44]]]
                              ["key5" [[2] nil]]
                              ["key1" [[2] [22]]]
                              ["key2" [[3] nil]]] :in-any-order))

      (fact
        "sample returns a fraction of the RDD, with/without replacement, Using a
        given random number generator seed, Sampling does not guarantee size but
        using same seed should return same result."
        (-> (f/parallelize c [0 1 2 3 4 5 6 7 8 9])
            (f/sample false 0.3 9)
            f/collect
            vec) => (-> (f/parallelize c [0 1 2 3 4 5 6 7 8 9])
                        (f/sample false 0.3 9)
                        f/collect
                        vec))

      (fact
        "combine-by-key returns an RDD by combining the elements for each key using a custom
        set of aggregation functions"
        (-> (f/parallelize c [["key1" 1]
                              ["key2" 1]
                              ["key1" 1]])
            (f/combine-by-key identity + +)
            f/collect
            vec) => (just [["key1" 2] ["key2" 1]] :in-any-order))

      (fact
        "sort-by-key returns an RDD of (K, V) pairs sorted by keys in asc or desc order"
        (-> (f/parallelize c [[2 "aa"]
                              [5 "bb"]
                              [3 "cc"]
                              [1 "dd"]])
            (f/sort-by-key compare false)
            f/collect
            vec) => [[5 "bb"] [3 "cc"] [2 "aa"] [1 "dd"]])

      (fact
        "coalesce"
        (-> (f/parallelize c [1 2 3 4 5])
            (f/coalesce 1)
            f/collect
            vec) => [1 2 3 4 5])

      (fact
        "group-by returns an RDD of items grouped by the grouping function"
        (-> (f/parallelize c [1 1 2 3 5 8])
            (f/group-by (f/fn [x] (mod x 2)))
            f/collect
            vec) => (just [[0 [2 8]] [1 [1 1 3 5]]] :in-any-order))

      (fact
        "group-by-key"
        (-> (f/parallelize c [["key1" 1]
                              ["key1" 2]
                              ["key2" 3]
                              ["key2" 4]
                              ["key3" 5]])
            f/group-by-key
            f/collect
            vec) => (just [["key3" [5]] ["key1" [1 2]] ["key2" [3 4]]] :in-any-order))

      (fact
        "flat-map-to-pair"
        (-> (f/parallelize c [["Four score and seven"]
                              ["years ago"]])
            (f/flat-map-to-pair (f/fn [x] (map (fn [y] [y 1])
                                               (clojure.string/split (first x) #" "))))
            (f/map f/untuple)
            f/collect
            vec) => [["Four" 1] ["score" 1] ["and" 1] ["seven" 1] ["years" 1] ["ago" 1]])

      (fact
        "map-partition"
        (-> (f/parallelize c [0 1 2 3 4])
            (f/map-partition (f/fn [it] (map identity (iterator-seq it))))
            f/collect) => [0 1 2 3 4])

      (fact
        "map-partition-with-index"
        (-> (f/parallelize c [0 1 2 3 4])
            (f/repartition 4)
            (f/map-partition-with-index (f/fn [i it] (.iterator (map identity (iterator-seq it)))))
            f/collect
            vec) => (just [0 1 2 3 4] :in-any-order))

      (fact
        "cartesian creates cartesian product of two RDDS"
        (let [rdd1 (f/parallelize c [1 2])
              rdd2 (f/parallelize c [5 6 7])]
          (-> (f/cartesian rdd1 rdd2)
            f/collect
            vec) => (just [[1 5] [1 6] [1 7] [2 5] [2 6] [2 7]] :in-any-order)))

      (future-fact "repartition returns a new RDD with exactly n partitions")

      )))

(facts
  "about actions"

  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name "api-test"))]
    (f/with-context c conf
      (fact
        "aggregates elements of RDD using a function that takes two arguments and returns one,
        return type is a value"
        (-> (f/parallelize c [1 2 3 4 5])
            (f/reduce (f/fn [x y] (+ x y)))) => 15)

      (fact
        "count-by-key returns a hashmap of (K, int) pairs with the count of each key; only available on RDDs of type (K, V)"
        (-> (f/parallelize c [["key1" 1]
                              ["key1" 2]
                              ["key2" 3]
                              ["key2" 4]
                              ["key3" 5]])
            (f/count-by-key)) => {"key1" 2 "key2" 2 "key3" 1})

      (fact
       "count-by-value returns a hashmap of (V, int) pairs with the count of each value"
       (-> (f/parallelize c [["key1" 11]
                             ["key1" 11]
                             ["key2" 12]
                             ["key2" 12]
                             ["key3" 13]])
           (f/count-by-value)) => {["key1" 11] 2, ["key2" 12] 2, ["key3" 13] 1})

       (fact
       "values returns the values (V) of a hashmap of (K, V) pairs from a JavaRDD"
       (-> (f/parallelize c [["key1" 11]
                             ["key1" 11]
                             ["key2" 12]
                             ["key2" 12]
                             ["key3" 13]])
           (f/values)
           (f/collect)
           vec) => [11, 11, 12, 12, 13])

       (fact
        "values returns the values (V) of a hashmap of (K, V) pairs from a JavaPairRDD"
        (-> (f/parallelize c [["key1" 11]
                              ["key1" 11]
                              ["key2" 12]
                              ["key2" 12]
                              ["key3" 13]])
            (f/map-to-pair identity)
            (f/values)
            (f/collect)
            vec) => [11, 11, 12, 12, 13])

      (fact
        "foreach runs a function on each element of the RDD, returns nil; this is usually done for side effcts"
        (-> (f/parallelize c [1 2 3 4 5])
            (f/foreach (f/fn [x] x))) => nil)

      (fact
        "foreach-partition runs a function on each partition iterator of RDD; basically for side effects like foreach"
        (-> (f/parallelize c [1 2 3 4 5])
            (f/foreach-partition (f/fn [it] (iterator-seq it)))) => nil)

      (fact
        "fold returns aggregate each partition, and then the results for all the partitions,
        using a given associative function and a neutral 'zero value'"
        (-> (f/parallelize c [1 2 3 4 5])
            (f/fold 0 (f/fn [x y] (+ x y)))) => 15)

      (fact
        "first returns the first element of an RDD"
        (-> (f/parallelize c [1 2 3 4 5])
            f/first) => 1)

      (fact
        "count return the number of elements in an RDD"
        (-> (f/parallelize c [["a" 1] ["b" 2] ["c" 3] ["d" 4] ["e" 5]])
            f/count) => 5)

      (fact
        "collect returns all elements of the RDD as an array at the driver program"
        (-> (f/parallelize c [[1] [2] [3] [4] [5]])
            f/collect
            vec) => [[1] [2] [3] [4] [5]])

      (fact
       "distinct returns distinct elements of an RDD"
       (-> (f/parallelize c [1 2 1 3 4 5 4])
           f/distinct
           f/collect
           vec) => (contains #{1 2 3 4 5}))

      (fact
       "distinct returns distinct elements of an RDD with the given number of partitions"
       (-> (f/parallelize c [1 2 1 3 4 5 4])
           (f/distinct 2)
           f/collect
           vec) => (contains #{1 2 3 4 5}))

      (fact
        "take returns an array with the first n elements of an RDD"
        (-> (f/parallelize c [1 2 3 4 5])
            (f/take 3)) => [1 2 3])

      (fact
        "glom returns an RDD created by coalescing all elements within each partition into a list"
        (-> (f/parallelize c [1 2 3 4 5 6 7 8 9 10] 2)
            f/glom
            f/collect
            vec) => (just [[1 2 3 4 5] [6 7 8 9 10]] :in-any-order))

      (fact
        "cache persists this RDD with a default storage level (MEMORY_ONLY)"
        (let [cache (-> (f/parallelize c [1 2 3 4 5])
                        (f/cache))]
          (-> cache
              f/collect) => [1 2 3 4 5]))

      (fact
       "histogram uses bucketCount number of evenly-spaced buckets"
       (-> (f/parallelize c [1.0 2.2 2.6 3.3 3.5 3.7 4.4 4.8 5.5 6.0])
           (f/histogram 5)) => [[1.0 2.0 3.0 4.0 5.0 6.0] [1 2 3 2 2]])

      (fact
       "histogram uses the provided buckets"
       (-> (f/parallelize c [1.0 2.2 2.6 3.3 3.5 3.7 4.4 4.8 5.5 6.0])
           (f/histogram [1.0 4.0 6.0])) => [6 4])
      )))
