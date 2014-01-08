(ns flambo.api
  (:refer-clojure :exclude [map reduce first count take distinct])
  (:require [serializable.fn :as sfn]
            [clojure.tools.logging :as log]
            [flambo.function :refer [flat-map-function
                                     function
                                     function2
                                     pair-function
                                     void-function]]
            [flambo.utils :as u])
  (:import (java.util Comparator)
           (org.apache.spark.api.java JavaSparkContext)))

(defn spark-context
  [& {:keys [master job-name spark-home jars environment]}]
  (log/warn "JavaSparkContext" master job-name spark-home jars environment)
  (JavaSparkContext. master job-name spark-home (into-array String jars) environment))

(defn local-spark-context [& {:keys [master job-name]}]
  (log/warn "JavaSparkContext" master job-name)
  (JavaSparkContext. master job-name))

(def untuple
  (sfn/fn [t]
    [(._1 t) (._2 t)]))

(def double-untuple
  (sfn/fn [t]
    (let [[x t2] (untuple t)]
      (vector x (untuple t2)))))

(defn ftruthy?
  [f]
  (sfn/fn [x] (u/truthy? (f x))))

(defn feach
  "Mostly useful for parsing a seq of Strings to their respective types.  Example
  (f/map (f/feach as-integer as-long identity identity as-integer as-double))
  Implies that each entry in the RDD is a sequence of 6 things.  The first element should be
  parsed as an Integer, the second as a Long, etc.  The actual functions supplied here can be
  any arbitray transformation (e.g. identity)."
  [& fs]
  (fn [coll]
    (clojure.core/map (fn [f x] (f x)) fs coll)))

;;; RDD construction

(defn text-file
  [spark-context filename]
  (.textFile spark-context filename))

(defn parallelize
  [spark-context lst]
  (.parallelize spark-context lst))

;;; Transformations

(defn map [rdd f]
  (.map rdd (function f)))

(defn reduce [rdd f]
  (.reduce rdd (function2 f)))

(defn flat-map [rdd f]
  (.map rdd (flat-map-function f)))

(defn foreach [rdd f]
  (.foreach rdd (void-function f)))

(defn aggregate [rdd zero-value seq-op comb-op]
  (.aggregate rdd zero-value (function2 seq-op) (function2 comb-op)))

(defn fold [rdd zero-value f]
  (.fold rdd zero-value (function2 f)))

(defn reduce-by-key [rdd f]
  (-> rdd
      (.map (pair-function identity))
      (.reduceByKey (function2 f))
      (.map (function untuple))
      ))

(defn group-by-key [rdd]
  (-> rdd
      (.map (pair-function identity))
      .groupByKey
      (.map (function untuple))))

(defn sort-by-key
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
         (.map (pair-function identity))
         (.sortByKey
          (if (instance? Comparator compare-fn)
            compare-fn
            (comparator compare-fn))
          (u/truthy? asc?))
         (.map (function untuple)))))

(defn join [rdd other]
  (-> rdd
      (.map (pair-function identity))
      (.join (.map other (pair-function identity)))
      (.map (function double-untuple))))

;;; Actions

(def first (memfn first))
(def count (memfn count))
(def glom (memfn glom))
(def cache (memfn cache))
(def collect (memfn collect))

;; take defined with memfn fails with an ArityException, so doing this instead:
(defn take [rdd cnt]
  (.take rdd cnt))

(def distinct (memfn distinct))
(def cache (memfn cache))
(def collect (memfn collect))
(def distinct (memfn distinct))
