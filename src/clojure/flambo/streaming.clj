;; ## EXPERIMENTAL
;;
;; This is a partial and mostly untested implementation of
;; Spark Streaming; consider it a work in progress.
;;
(ns flambo.streaming
  (:refer-clojure :exclude [map time print union count])
  (:require [flambo.api :as f]
            [flambo.conf :as conf]
            [flambo.function :refer [flat-map-function
                                     function
                                     function2
                                     pair-function
                                     void-function]])
  (:import [org.apache.spark.streaming.api.java JavaStreamingContext]
           [org.apache.spark.streaming.kafka KafkaUtils]
           [org.apache.spark.streaming.flume FlumeUtils]
           [org.apache.spark.streaming Duration Time]))

(defn duration [ms]
  (Duration. ms))

(defn time [ms]
  (Time. ms))

(defn streaming-context
  "conf can be a SparkConf or JavaSparkContext"
  [conf batch-duration]
  (JavaStreamingContext. conf (duration batch-duration)))

(defn local-streaming-context [app-name duration]
  (let [conf (-> (conf/spark-conf)
                 (conf/master "local")
                 (conf/app-name app-name))]
    (streaming-context conf duration)))

(defmulti checkpoint (fn [context arg] (class arg)))
(defmethod checkpoint String [streaming-context path] (.checkpoint streaming-context path))
(defmethod checkpoint Long [dstream interval] (.checkpoint dstream (duration interval)))

(defn text-file-stream [context file-path]
  (.textFileStream context file-path))

(defn socket-text-stream [context ip port]
  (.socketTextStream context ip port))

(defn kafka-stream [& {:keys [streaming-context zk-connect group-id topic-map]}]
  (KafkaUtils/createStream streaming-context zk-connect group-id (into {} (for [[k, v] topic-map] [k (Integer. v)]))))

(defn flume-stream [streaming-context host port]
  (FlumeUtils/createStream streaming-context host port))

(defn flume-polling-stream [streaming-context host port]
  (FlumeUtils/createPollingStream streaming-context host port))

(defn flat-map [dstream f]
  (.flatMap dstream (flat-map-function f)))

(defn map [dstream f]
  (.map dstream (function f)))

(defn reduce-by-key [dstream f]
  "Call reduceByKey on dstream of type JavaPairDStream"
  (.reduceByKey dstream (function2 f)))

(defn map-to-pair [dstream f]
  (.mapToPair dstream (pair-function f)))


;; ## Transformations
;;
(defn transform [dstream f]
  (.transform dstream (function f)))

(defn repartition [dstream num-partitions]
  (.repartition dstream (Integer. num-partitions)))

(defn union [dstream other-stream]
  (.union dstream other-stream))


;; ## Window Operations
;;
(defn window [dstream window-length slide-interval]
  (.window dstream (duration window-length) (duration slide-interval)))

(defn count [dstream]
  (.count dstream))

(defn count-by-window [dstream window-length slide-interval]
  (.countByWindow dstream (duration window-length) (duration slide-interval)))

(defn group-by-key-and-window [dstream window-length slide-interval]
  (.groupByKeyAndWindow dstream (duration window-length) (duration slide-interval)))

(defn reduce-by-window [dstream f f-inv window-length slide-interval]
  (.reduceByWindow dstream (function2 f) (function2 f-inv) (duration window-length) (duration slide-interval)))

(defn reduce-by-key-and-window [dstream f window-length slide-interval]
  (.reduceByKeyAndWindow dstream (function2 f) (duration window-length) (duration slide-interval)))


;; ## Actions
;;
(def print (memfn print))

(defn foreach-rdd [dstream f]
  (.foreachRDD dstream (function2 f)))


;; ## Output
;;
(defn save-as-text-files
  ;;TODO: check whether first param is of type
  ;;DStream or just let an exception be thrown?
  ([dstream prefix suffix]
    (.saveAsTextFiles dstream prefix suffix))
  ([dstream prefix]
   (.saveAsTextFiles dstream prefix)))
