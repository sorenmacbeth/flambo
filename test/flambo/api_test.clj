(ns flambo.api-test
  (:use midje.sweet)
  (:require [flambo.api :as f]))

(defmacro with-context [context-sym & body]
  `(let [~context-sym (f/spark-context "local[*]" "test")]
     ~@body
     (.stop ~context-sym)))

(facts
 "about spark-context"
 (with-context c
   "gives us a JavaSparkContext"
   (class c) => org.apache.spark.api.java.JavaSparkContext

   "creates a JavaRDD"
   (f/parallelize c [1 2 3 4 5]) => #(instance? org.apache.spark.api.java.JavaRDD %)

   "round-trips a clojure vector"
   (-> (f/parallelize c [1 2 3 4 5]) f/collect vec) => (just [1 2 3 4 5])))

(facts
 "about Tuple2")
