(ns flambo.conf-test
  (:use midje.sweet)
  (:require [flambo.conf :as conf]))

(facts
 "about setting k/v into spark-conf"
 (let [c (conf/spark-conf)]
   (fact
    "spark-conf returns a SparkConf object"
    (class c) => org.apache.spark.SparkConf)

   (fact
    "setting master works"
    (conf/get (conf/master c "local") "spark.master") => "local")

   (fact
    "setting app-name works"
    (conf/get (conf/app-name c "test") "spark.app.name") => "test")))
