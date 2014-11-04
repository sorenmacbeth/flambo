(ns flambo.api-test
  (:use clojure.test)
  (:require [flambo.api :as f]
            [flambo.conf :as conf]))

(deftest spark-context
  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name "api-test"))]
    (f/with-context c conf
      (testing "gives us a JavaSparkContext"
        (is (= (class c) org.apache.spark.api.java.JavaSparkContext)))

      (testing "creates a JavaRDD"
        (is (= (class (f/parallelize c [1 2 3 4 5]))
               org.apache.spark.api.java.JavaRDD)))

      (testing "round-trips a clojure vector"
        (is (= (-> (f/parallelize c [1 2 3 4 5]) f/collect vec)
               [1 2 3 4 5]))))))

(deftest untupling
  (testing "untuple returns a 2 vector"
    (let [tuple2 (scala.Tuple2. 1 "hi")]
      (is (= (f/untuple tuple2) [1 "hi"]))))

  (testing "double untuple returns a vector with a key and a 2 vector value"
    (let [double-tuple2 (scala.Tuple2. 1 (scala.Tuple2. 2 "hi"))]
      (is (= (f/double-untuple double-tuple2) [1 [2 "hi"]])))))

(deftest transformations
  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name "api-test"))]
    (f/with-context c conf
      (testing "map returns an RDD formed by passing each element of the source RDD through a function"
        (is (= (-> (f/parallelize c [1 2 3 4 5])
                   (f/map (f/fn [x] (* 2 x)))
                   f/collect
                   vec)
               [2 4 6 8 10])))

      (testing "map-to-pair returns an RDD of (K, V) pairs formed by passing each element of the source
                RDD through a pair function"
        (is (= (-> (f/parallelize c ["a" "b" "c" "d"])
                   (f/map-to-pair (f/fn [x] [x 1]))
                   (f/map f/untuple)
                   f/collect
                   vec)
                [["a" 1] ["b" 1] ["c" 1] ["d" 1]])))

      (testing "reduce-by-key returns an RDD of (K, V) when called on an RDD of (K, V) pairs"
        (is (= (-> (f/parallelize c [["key1" 1]
                                     ["key1" 2]
                                     ["key2" 3]
                                     ["key2" 4]
                                     ["key3" 5]])
                   (f/reduce-by-key (f/fn [x y] (+ x y)))
                   f/collect
                   vec)
                [["key3" 5] ["key1" 3] ["key2" 7]])))

      (testing "similar to map, but each input item can be mapped to 0 or more output items;
                mapping function must therefore return a sequence rather than a single item"
        (is (= (-> (f/parallelize c [["Four score and seven years ago our fathers"]
                                     ["brought forth on this continent a new nation"]])
                   (f/flat-map (f/fn [x] (clojure.string/split (first x) #" ")))
                   f/collect
                   vec)
               ["Four" "score" "and" "seven" "years" "ago" "our"
                "fathers" "brought" "forth" "on" "this" "continent"
                "a" "new" "nation"])))

      (testing "filter returns an RDD formed by selecting those elements of the source on
                which func returns true"
        (is (= (-> (f/parallelize c [1 2 3 4 5 6])
                   (f/filter (f/fn [x] (even? x)))
                   f/collect
                   vec)
               [2 4 6])))

      (testing "join returns an RDD of (K, (V, W)) pairs with all pairs of elements of each
                key when called on RDDs of type (K, V) and (K, W)"
        (let [LDATA (f/parallelize c [["key1" [2]]
                                      ["key2" [3]]
                                      ["key3" [5]]
                                      ["key4" [1]]
                                      ["key5" [2]]])
              RDATA (f/parallelize c [["key1" [22]]
                                      ["key3" [33]]
                                      ["key4" [44]]])]
          (is (= (-> (f/join LDATA RDATA)
                     f/collect
                     vec))
                 [["key3" [[5] [33]]]
                  ["key4" [[1] [44]]]
                  ["key1" [[2] [22]]]])))

      (testing "left-outer-join returns an RDD of (K, (V, W))
                when called on RDDs of type (K, V) and (K, W)"
        (let [LDATA (f/parallelize c [["key1" [2]]
                                      ["key2" [3]]
                                      ["key3" [5]]
                                      ["key4" [1]]
                                      ["key5" [2]]])
              RDATA (f/parallelize c [["key1" [22]]
                                      ["key3" [33]]
                                      ["key4" [44]]])]
          (is (= (-> (f/left-outer-join LDATA RDATA)
                     f/collect
                     vec)
                 [["key3" [[5] [33]]]
                  ["key4" [[1] [44]]]
                  ["key5" [[2] nil]]
                  ["key1" [[2] [22]]]
                  ["key2" [[3] nil]]]))))

      (testing "sample returns a fraction of the RDD, with/without replacement,
                using a given random number generator seed"
        (is (= (-> (f/parallelize c [0 1 2 3 4 5 6 7 8 9])
                   (f/sample false 0.1 2)
                   f/collect
                   vec)
                [6])))

      (testing "combine-by-key returns an RDD by combining the elements for each key using a custom
                set of aggregation functions"
        (is (= (-> (f/parallelize c [["key1" 1]
                                     ["key2" 1]
                                     ["key1" 1]])
                   (f/combine-by-key identity + +)
                   f/collect
                   vec)
               [["key1" 2] ["key2" 1]])))

      (testing "sort-by-key returns an RDD of (K, V) pairs sorted by keys in asc or desc order"
        (is (= (-> (f/parallelize c [[2 "aa"]
                                     [5 "bb"]
                                     [3 "cc"]
                                     [1 "dd"]])
                   (f/sort-by-key compare false)
                   f/collect
                   vec)
               [[5 "bb"] [3 "cc"] [2 "aa"] [1 "dd"]])))

      (testing "coalesce"
        (is (= (-> (f/parallelize c [1 2 3 4 5])
                   (f/coalesce 1)
                   f/collect
                   vec)
               [1 2 3 4 5])))

      (testing "group-by returns an RDD of items grouped by the grouping function"
        (is (= (-> (f/parallelize c [1 1 2 3 5 8])
                   (f/group-by (f/fn [x] (mod x 2)))
                   f/collect
                   vec)
               [[0 [2 8]] [1 [1 1 3 5]]])))

      (testing "group-by-key"
        (is (= (-> (f/parallelize c [["key1" 1]
                                     ["key1" 2]
                                     ["key2" 3]
                                     ["key2" 4]
                                     ["key3" 5]])
                   f/group-by-key
                   f/collect
                   vec)
               [["key3" [5]] ["key1" [1 2]] ["key2" [3 4]]])))

      (testing "flat-map-to-pair"
        (is (= (-> (f/parallelize c [["Four score and seven"]
                                     ["years ago"]])
                   (f/flat-map-to-pair (f/fn [x] (map (fn [y] [y 1])
                                                      (clojure.string/split (first x) #" "))))
                   (f/map f/untuple)
                   f/collect
                   vec)
               [["Four" 1] ["score" 1] ["and" 1] ["seven" 1] ["years" 1] ["ago" 1]])))

      (testing "repartition returns a new RDD with exactly n partitions"))))

(deftest actions
  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name "api-test"))]
    (f/with-context c conf
      (testing "aggregates elements of RDD using a function that takes two arguments and returns one,
                return type is a value"
        (is (= (-> (f/parallelize c [1 2 3 4 5])
                   (f/reduce (f/fn [x y] (+ x y))))
               15)))

      (testing "count-by-key returns a hashmap of (K, int) pairs with the count of each key;
                only available on RDDs of type (K, V)"
        (is (= (-> (f/parallelize c [["key1" 1]
                                     ["key1" 2]
                                     ["key2" 3]
                                     ["key2" 4]
                                     ["key3" 5]])
                   (f/count-by-key))
               {"key1" 2 "key2" 2 "key3" 1})))

      (testing "count-by-value returns a hashmap of (V, int) pairs with the count of each value"
        (is (= (-> (f/parallelize c [["key1" 11]
                                     ["key1" 11]
                                     ["key2" 12]
                                     ["key2" 12]
                                     ["key3" 13]])
                   (f/count-by-value))
               {["key1" 11] 2, ["key2" 12] 2, ["key3" 13] 1})))

      (testing "foreach runs a function on each element of the RDD, returns nil;
                this is usually done for side effcts"
        (is (= (-> (f/parallelize c [1 2 3 4 5])
                   (f/foreach (f/fn [x] x)))
               nil)))

      (testing "fold returns aggregate each partition, and then the results for all the partitions,
                using a given associative function and a neutral 'zero value'"
        (is (= (-> (f/parallelize c [1 2 3 4 5])
                   (f/fold 0 (f/fn [x y] (+ x y))))
               15)))

      (testing "first returns the first element of an RDD"
        (is (= (-> (f/parallelize c [1 2 3 4 5])
                   f/first)
               1)))

      (testing "count return the number of elements in an RDD"
        (is (= (-> (f/parallelize c [["a" 1] ["b" 2] ["c" 3] ["d" 4] ["e" 5]])
                   f/count)
               5)))

      (testing "collect returns all elements of the RDD as an array at the driver program"
        (is (= (-> (f/parallelize c [[1] [2] [3] [4] [5]])
                   f/collect
                   vec)
               [[1] [2] [3] [4] [5]])))

      (testing "distinct returns distinct elements of an RDD"
        (is (= (-> (f/parallelize c [1 2 1 3 4 5 4])
                   f/distinct
                   f/collect
                   vec
                   sort)
               [1 2 3 4 5])))

      (testing "distinct returns distinct elements of an RDD with the given number of partitions"
        (is (= (-> (f/parallelize c [1 2 1 3 4 5 4])
                   (f/distinct 2)
                   f/collect
                   vec
                   sort)
               [1 2 3 4 5])))

      (testing "take returns an array with the first n elements of an RDD"
        (is (= (-> (f/parallelize c [1 2 3 4 5])
                   (f/take 3))
               [1 2 3])))

      (testing "glom returns an RDD created by coalescing all elements within each partition into a list"
        (is (= (-> (f/parallelize c [1 2 3 4 5 6 7 8 9 10] 2)
                   f/glom
                   f/collect
                   vec)
               [[1 2 3 4 5] [6 7 8 9 10]])))

      (testing "cache persists this RDD with a default storage level (MEMORY_ONLY)"
        (let [cache (-> (f/parallelize c [1 2 3 4 5])
                        (f/cache))]
          (is (= (-> cache f/collect)
                 [1 2 3 4 5]))))
      )))
