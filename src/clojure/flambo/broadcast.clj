(ns flambo.broadcast
  (:import [org.apache.spark.broadcast Broadcast]))

(defn broadcast [sc value]
  (.broadcast sc value))

(defn value [^Broadcast broadcast-var]
  (.value broadcast-var))
