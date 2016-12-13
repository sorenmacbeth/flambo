;; Functions for creating and modifying `SparkSession` objects
;;
;; The functions are designed to be used with the threading macro `->` for building.
;;
;; (-> (spark-session)
;;      (app-name "aname")
;;      (master "local[2]"))
;;
(ns flambo.session
  (:import [org.apache.spark.sql SparkSession SparkSession$Builder]))

(defn session-builder []
  (SparkSession/builder))

(defn master
  ([sb]
   (master sb "local[*]"))
  ([sb master]
   (.master sb master)))

(defn app-name
  [sb name]
  (.appName sb name))

(defn config
  ([sb spark-conf]
   (.config sb spark-conf))
  ([sb k v]
   (.config sb k v)))

(defn get-or-create
  [sb]
  (.getOrCreate sb))
