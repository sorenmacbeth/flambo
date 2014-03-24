(ns flambo.streaming
  (:refer-clojure :exclude [map time print union])
  (:require [flambo.api :as f]
            [flambo.conf :as conf]
            [flambo.function :refer [flat-map-function
                                     function
                                     function2
                                     pair-function
                                     void-function]])
  (:import [org.apache.spark.streaming.api.java JavaStreamingContext]
           [org.apache.spark.streaming.kafka KafkaUtils]
           [org.apache.spark.streaming Duration Time]))

(defn duration [ms]
  (Duration. ms))

(defn time [ms]
  (Time. ms))

(defn streaming-context [conf batch-duration]
  (JavaStreamingContext. conf (duration batch-duration)))

(defn local-streaming-context [app-name duration]
  (let [conf (-> (conf/spark-conf)
                 (conf/master "local")
                 (conf/app-name app-name))]
    (streaming-context conf duration)))

(defn socket-text-stream [context ip port]
  (.socketTextStream context ip port))

(defn kafka-stream [& {:keys [streaming-context zk-connect group-id topic-map]}]
  (KafkaUtils/createStream streaming-context zk-connect group-id (into {} (for [[k, v] topic-map] [k (Integer. v)]))))

(defn flat-map [dstream f]
  (.flatMap dstream (flat-map-function f)))

(defn map [dstream f]
  (.map dstream (function f)))

(defn reduce-by-key [dstream f]
  (-> dstream
      (.map (pair-function identity))
      (.reduceByKey (function2 f))
      (.map (function f/untuple))))


;; transformations

(defn transform [dstream f]
  (.transform dstream (function f)))

(defn repartition [dstream num-partitions]
  (.repartition dstream (Integer. num-partitions)))

(defn union [dstream other-stream]
  (.union dstream other-stream))


;; window operations

(defn window [dstream window-length slide-interval]
  (.window dstream (duration window-length) (duration slide-interval)))

(defn count-by-window [dstream window-length slide-interval]
  (.countByWindow dstream (duration window-length) (duration slide-interval)))

(defn group-by-key-and-window [dstream window-length slide-interval]
  (-> dstream
      (.map (pair-function identity))
      (.groupByKeyAndWindow (duration window-length) (duration slide-interval))
      (.map (function f/untuple))))

(defn reduce-by-window [dstream f window-length slide-interval]
  (.reduceByWindow dstream (function2 f) (duration window-length) (duration slide-interval)))

;(defn reduce-by-key-and-window [dstream f window-length slide-interval]
;  (-> dstream
;      (.map (pair-function identity))
;      (.reduceByKeyAndWindow (function2 f) (duration window-length) (duration slide-interval))
;      (.map (function f/untuple))))


;; output operations

(def print (memfn print))

(defn foreach-rdd [dstream f]
  (.foreachRDD dstream (function2 f)))
