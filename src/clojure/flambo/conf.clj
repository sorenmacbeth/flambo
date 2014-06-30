;; Functions for creating and modifying `SparkConf` objects
;;
;; The functions are designed to be used with the threading macro `->` for building
;; objects to pass into `spark-context`.
;;
;; (-> (spark-conf)
;;      (app-name "aname")
;;      (master "local[2]"))
;;
(ns flambo.conf
  (:import [org.apache.spark SparkConf])
  (:refer-clojure :exclude (set get contains remove)))

(defn spark-conf
  []
  (SparkConf.))

(defn master
  ([conf]
     (master conf "local[*]"))
  ([conf master]
     (.setMaster conf master)))

(defn app-name
  [conf name]
  (.setAppName conf name))

(defn jars
  [conf jars]
  (.setJars conf (into-array String jars)))

(defn set
  ([conf key val]
     (.set conf key val))
  ([conf amap]
     (loop [c conf
            aseq (seq amap)]
       (if aseq
         (let [[k v] (first aseq)
               c (set c k v)]
           (recur c (next aseq)))
         c))))

(defn set-if-missing
  [conf key val]
  (.setIfMissing key val))

(defn set-executor-env
  ([conf key val]
     (.setExecutorEnv conf key val))
  ([conf amap]
     (loop [c conf
            aseq (seq amap)]
       (if aseq
         (let [[k v] (first aseq)
               c (set-executor-env c k v)]
           (recur c (next aseq)))
         c))))

(defn remove
  [conf key]
  (.remove conf key))

(defn get
  ([conf key]
     (.get conf key))
  ([conf key default]
     (.get conf key default)))

(defn get-all
  [conf]
  (into {} (for [t (.getAll conf)]
             {(._1 t) (._2 t)})))

(defn spark-home
  [conf home]
  (.setSparkHome conf home))

(defn to-string
  [conf]
  (.toDebugString conf))
