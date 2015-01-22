(ns flambo.tuple
  (:refer-clojure :exclude [first second seq vec])
  (:require [serializable.fn :as sfn])
  (:import [scala Tuple2]))

(defprotocol ClojureTuple2
  (first [this])
  (second [this])
  (vec [this])
  (seq [this]))

(extend-type Tuple2
  ClojureTuple2
  (first [this]
    (._1 this))
  (second [this]
    (._2 this))
  (vec [this]
    (let [v (transient [])]
      (conj! v (first this))
      (conj! v (second this))
      (persistent! v)))
  (seq [this]
    (clojure.core/seq (vec this))))

(defn tuple [k v]
  (Tuple2. k v))

(defn key-val-fn [f]
  (sfn/fn [^Tuple2 t]
    (f (first t) (second t))))

(defn key-val-val-fn [f]
  (sfn/fn [^Tuple2 t]
    (let [k (first t)
          v ^Tuple2 (second t)]
      (f k (first v) (second v)))))
