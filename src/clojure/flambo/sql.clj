;; ## EXPERIMENTAL
;;
;; This code as well as SparkSQL itself are considered experimental.
;;
(ns flambo.sql
  (:import [org.apache.spark.sql.api.java JavaSQLContext]))

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

;; ## JavaSchemaRDD
;;
(defn register-as-table [rdd table-name]
  (.registerAsTable rdd table-name))

(def print-schema (memfn printSchema))
