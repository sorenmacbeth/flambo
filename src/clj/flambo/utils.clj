(ns flambo.utils
  (:import (scala Tuple2)))

(defn to-spec [avar]
  (let [m (meta avar)]
    [(str (:ns m)) (str (:name m))]))

(defn tuple [[a b]]
  (Tuple2. a b))

(defn untuple [t]
  [(._1 t) (._2 t)])

(defn double-untuple
  "Convert (k, (v, w)) to [k [v w]]."
  [t]
  (let [[x t2] (untuple t)]
    (vector x (untuple t2))))

(defn truthy? [x]
  (if x (Boolean. true) (Boolean. false)))

(defn as-integer [s]
  (Integer. s))

(defn as-long [s]
  (Long. s))

(defn as-double [s]
  (Double. s))
