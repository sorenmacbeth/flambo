(ns flambo.conf-test
  (:use clojure.test)
  (:require [flambo.conf :as conf]))

(deftest spark-conf
  "about setting k/v into spark-conf"
  (let [c (conf/spark-conf)]
    (testing "spark-conf returns a SparkConf object"
      (is (= (class c) org.apache.spark.SparkConf)))

    (testing "setting master works"
      (is (= (conf/get (conf/master c "local") "spark.master") "local")))

    (testing "setting app-name works"
      (is (= (conf/get (conf/app-name c "test") "spark.app.name") "test")))))
