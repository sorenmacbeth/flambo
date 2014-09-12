;; ## EXPERIMENTAL
;;
;; This code as well as SparkSQL itself are considered experimental.
;;
(ns flambo.sql
  (:require [flambo.api :as f :refer [defsparkfn]])
  (:import [org.apache.spark.sql.api.java JavaSQLContext Row]
           [org.apache.spark.sql SQLContext]))

;; ## JavaSQLContext
;;
(defn sql-context [spark-context]
  (JavaSQLContext. spark-context))

(defn sql [sql-context query]
  (.sql sql-context query))

(defn parquet-file [sql-context path]
  (.parquetFile sql-context path))

(defn json-file [sql-context path]
  (.jsonFile sql-context path))

(defn register-rdd-as-table [sql-context rdd table-name]
  (.registerRDDAsTable sql-context rdd table-name))

(defn cache-table [sql-context table-name]
  (let [scala-sql-context (.sqlContext sql-context)]
    (.cacheTable scala-sql-context table-name)))

;; ## JavaSchemaRDD
;;
(defn register-as-table [rdd table-name]
  (.registerAsTable rdd table-name))

(def print-schema (memfn printSchema))

;; ## Row
;;
(defsparkfn row->vec [^Row row]
  (let [n (.length row)]
    (loop [i 0 v (transient [])]
      (if (< i n)
        (recur (inc i) (conj! v (.get row i)))
        (persistent! v)))))
