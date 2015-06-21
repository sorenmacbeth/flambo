(ns flambo.tuple
  (:require [serializable.fn :as sfn])
  (:import [scala Tuple2]
           [flambo.function AbstractFlamboFunction]
           [java.io Writer]))

(set! *warn-on-reflection* true)

(defn ^Tuple2 tuple [k v]
  (Tuple2. k v))

(defn key-val-fn [^AbstractFlamboFunction f]
  (sfn/fn [^Tuple2 t]
    (f (._1 t) (._2 t))))

(defn key-val-val-fn [^AbstractFlamboFunction f]
  (sfn/fn [^Tuple2 t]
    (let [k (._1 t)
          v ^Tuple2 (._2 t)]
      (f k (._1 v) (._2 v)))))

(defmethod print-method Tuple2 [^Tuple2 o ^Writer w]
  (.write w (str "#flambo/tuple " (pr-str [(._1 o) (._2 o)]))))

(defmethod print-dup Tuple2 [o w]
  (print-method o w))
