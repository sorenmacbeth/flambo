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
      (-> (f/parallelize c [1 2 3 4 5]) f/collect vec) => (just [1 2 3 4 5])))))

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
        (-> (f/parallelize c ["Four score and seven years ago our fathers"
                              "brought forth on this continent a new nation"])
            (f/flat-map (f/fn [x] (clojure.string/split x #" ")))
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
              vec)) => [["key3" [[5] [33]]]
                        ["key4" [[1] [44]]]
                        ["key1" [[2] [22]]]])

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
              vec)) => [["key3" [[5] [33]]]
                        ["key4" [[1] [44]]]
                        ["key5" [[2] nil]]
                        ["key1" [[2] [22]]]
                        ["key2" [[3] nil]]])

      #_(fact
        ""
        (-> (f/parallelize c ["Four score and seven years ago"])
            (f/flat-map-to-pair (f/fn [x] (map (fn [x] [x 1])
                                                    (clojure.string/split x #" "))))
            (f/map f/double-untuple)
            f/collect
            vec) => [["Four" 1] ["score" 1] ["and" 1] ["seven" 1] ["years" 1] ["ago" 1]])
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
            (f/count-by-key)) => {"key1" 2, "key2" 2, "key3" 1})
      )))
