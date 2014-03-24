(ns flambo.streaming
  (:require [serializable.fn :as sfn]
            [flambo.api :as api]
            [flambo.utils :as u]
            [flambo.kryo :as kryo]
            [clojure.tools.logging :as log])
  (:import [scala Tuple2]
           [org.apache.spark.streaming Duration]
           [org.apache.spark.streaming.kafka KafkaUtils]
           (org.apache.spark.streaming.api.java JavaStreamingContext)))

(defn duration
  [msecs]
  (Duration. msecs))

(defn spark-streaming-context
  [& {:keys [master app-name batch-duration spark-home jars environment]}]
  (log/warn "JavaStreamingContext" master app-name batch-duration spark-home jars environment)
  (JavaStreamingContext. master app-name (duration batch-duration) spark-home (into-array String jars) environment))

(defn local-spark-streaming-context [& {:keys [master app-name batch-duration]}]
  (log/warn "JavaStreamingContext" master app-name batch-duration)
  (JavaStreamingContext. master app-name (duration batch-duration)))

(defn file-stream [d-stream data-directory]
  (.fileStream d-stream data-directory))

(defn kafka-stream [& {:keys [ssc zk-connect group-id topic-map]}]
  (KafkaUtils/createStream ssc zk-connect group-id (into {} (for [[k, v] topic-map] [k (Integer. v)]))))
