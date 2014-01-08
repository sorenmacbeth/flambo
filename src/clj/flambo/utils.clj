(ns flambo.utils
  (:import (scala Tuple2)))

(defn truthy? [x]
  (if x (Boolean. true) (Boolean. false)))

(defn as-integer [s]
  (Integer. s))

(defn as-long [s]
  (Long. s))

(defn as-double [s]
  (Double. s))
