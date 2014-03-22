(ns flambo.conf
  (:import [org.apache.spark SparkConf])
  (:refer-clojure :exclude (set)))

;; SparkConf functions

(defn spark-conf []
  (SparkConf.))

(defn master [conf master]
  (.setMaster conf master))

(defn app-name [conf name]
  (.setAppName conf name))

(defn jars [conf jars]
  (.setJars conf (into-array String jars)))

(defn set [conf key val]
  (.set conf key val))

(defn spark-home [conf home]
  (.setSparkHome conf home))

(defn to-string [conf]
  (.toDebugString conf))
