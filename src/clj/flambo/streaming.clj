(ns flambo.streaming
  (:refer-clojure :exclude [map time])
  (:require [flambo.api :as f]
            [flambo.function :refer [flat-map-function
                                     function
                                     function2
                                     pair-function
                                     void-function]])
  (:import [org.apache.spark.streaming.api.java JavaStreamingContext]
           [org.apache.spark.streaming Duration Time]))

(defn duration [ms]
  (Duration. ms))

(defn time [ms]
  (Time. ms))

(defn streaming-context [conf duration]
  (JavaStreamingContext. conf duration))

(defn socket-text-stream [context ip port]
  (.socketTextStream context ip port))

(defn flat-map [dstream f]
  (.flatMap dstream (flat-map-function f)))

(defn map [dstream f]
  (.map dstream (function f)))

(defn reduce-by-key [dstream f]
  (-> dstream
      (.map (pair-function identity))
      (.reduceByKey (function2 f))
      (.map (function f/untuple))))
