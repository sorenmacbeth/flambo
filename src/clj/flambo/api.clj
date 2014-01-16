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

(u/hail-flambo)

(System/setProperty "spark.serializer", "org.apache.spark.serializer.KryoSerializer")
(System/setProperty "spark.kryo.registrator", "flambo.kryo.CarboniteRegistrator")

(defmacro sparkop [& body]
  `(sfn/fn ~@body))

(defmacro defsparkfn [name & body]
  `(def ~name
     (sparkop ~@body)))

(defn spark-context
  [& {:keys [master job-name spark-home jars environment]}]
  (log/warn "JavaSparkContext" master job-name spark-home jars environment)
  (JavaSparkContext. master job-name spark-home (into-array String jars) environment))

(defn local-spark-context [& {:keys [master job-name]}]
  (log/warn "JavaSparkContext" master job-name)
  (JavaSparkContext. master job-name))

(defsparkfn untuple [t]
  [(._1 t) (._2 t)])

(defsparkfn double-untuple [t]
  (let [[x t2] (untuple t)]
    (vector x (untuple t2))))

(defn ftruthy? [f]
  (sparkop [x] (u/truthy? (f x))))

;;; RDD construction

(defn text-file [spark-context filename]
  (.textFile spark-context filename))

(defn parallelize  [spark-context lst] (.parallelize spark-context lst)
  
  )

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

(defn sample [rdd with-replacement? fraction seed]
    (-> rdd
        (.map (pair-function identity))
        (.sample with-replacement? fraction seed)
        (.map (function untuple))))

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
