(ns flambo.sql-functions
  (:import [org.apache.spark.sql functions]))


(defmulti col class)
(defmethod col org.apache.spark.sql.Column [x] x)
(defmethod col java.lang.String [x] (functions/col x))
(defmethod col clojure.lang.Symbol [x] (functions/col (str x)))
