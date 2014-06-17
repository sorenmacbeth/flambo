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

 (let [myfn (f/sparkop [x] (* 2 x))]
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
