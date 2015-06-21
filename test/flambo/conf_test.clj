(ns flambo.conf-test
  (:use midje.sweet)
  (:require [flambo.conf :as conf])
  (:import (org.apache.spark SparkConf)))

(facts
 "about setting k/v into spark-conf"
 (let [c (conf/spark-conf)]
   (fact
    "spark-conf returns a SparkConf object"
    (class c) => SparkConf)

   (fact
    "setting master works"
    (conf/get (conf/master c "local") "spark.master") => "local")

   (fact
    "setting app-name works"
    (conf/get (conf/app-name c "test") "spark.app.name") => "test")))
