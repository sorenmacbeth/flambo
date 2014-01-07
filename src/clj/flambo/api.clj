(ns flambo.api
  (:require [clojure.tools.logging :as log]
            [flambo.function :refer [defpairfn defsparkfn]]
            [flambo.utils :as u])
  (:import (org.apache.spark.api.java JavaSparkContext))
  (:refer-clojure :exclude
                  [count distinct filter first identity map reduce
                         take]))

(defsparkfn identity [x] (clojure.core/identity x))

(defsparkfn untuple [x] (u/untuple x))

(defpairfn pidentity [x] (clojure.core/identity x))

(defn spark-context
  [& {:keys [master job-name spark-home jars environment]}]
  (log/warn "JavaSparkContext" master job-name spark-home jars environment)
  (JavaSparkContext. master job-name spark-home (into-array String jars) environment))

(defn text-file
  [spark-context filename]
  (.textFile spark-context filename))

(defn parallelize
  [spark-context lst]
  (.parallelize spark-context lst))

(defn map [rdd f]
  (.map rdd f))

(defn reduce [rdd f]
  (.reduce rdd f))

(defn filter [rdd f]
  (.filter rdd f))

(defn foreach [rdd f]
  (.foreach rdd f))

(defn take [rdd cnt]
  (.take rdd cnt))

(def first (memfn first))

(def count (memfn count))

(def glom (memfn glom))

(def cache (memfn cache))

(def collect (memfn collect))

(def distinct (memfn distinct))
