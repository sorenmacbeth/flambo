;; ## EXPERIMENTAL
;;
;; This code as well as SparkSQL itself are considered experimental.
;;
(ns flambo.sql
  (:require [flambo.api :as f :refer [defsparkfn]])
  (:import [org.apache.spark.sql SQLContext Row]))

;; ## JavaSQLContext
;;
(defn sql-context [spark-context]
  (SQLContext. spark-context))

(defn sql [sql-context query]
  (.sql sql-context query))

(defn parquet-file [sql-context path]
  (.parquetFile sql-context path))

(defn json-file [sql-context path]
  (.jsonFile sql-context path))

(defn register-data-frame-as-table [sql-context df table-name]
  (.registerDataFrameAsTable sql-context df table-name))

(defn cache-table [sql-context table-name]
  (.cacheTable sql-context table-name))

;; ## DataFrame
;;
(defn register-temp-table [df table-name]
  (.registerTempTable df table-name))

(def print-schema (memfn printSchema))

;; ## Row
;;
(defsparkfn row->vec [^Row row]
  (let [n (.length row)]
    (loop [i 0 v (transient [])]
      (if (< i n)
        (recur (inc i) (conj! v (.get row i)))
        (persistent! v)))))
