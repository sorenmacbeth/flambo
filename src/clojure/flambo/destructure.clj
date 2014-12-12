(ns flambo.destructure
  (:require [serializable.fn :as sfn])
  (:import [scala Tuple2]))

(defn tuple-fn [f]
  (sfn/fn [^Tuple2 t]
    (f (._1 t) (._2 t))))

(defn tuple-value-fn [f]
  (sfn/fn [^Tuple2 t]
    (let [k (._1 t)
          v ^Tuple2 (._2 t)]
      (f k (._1 v) (._2 v)))))
