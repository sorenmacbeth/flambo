(ns flambo.sql-functions-test
  (:use midje.sweet)
  (:require [flambo.api   :as f]
            [flambo.conf  :as conf]
            [flambo.sql   :as sql]
            [flambo.sql-functions :as sqlf]))

(facts
 "about spark-sql-functions"
 (fact
  "col gives a Column"
  (class (sqlf/col "foo")) => org.apache.spark.sql.Column
  (class (sqlf/col 'foo)) => org.apache.spark.sql.Column
  (class (sqlf/col (org.apache.spark.sql.Column. "foo"))) => org.apache.spark.sql.Column)
 )
