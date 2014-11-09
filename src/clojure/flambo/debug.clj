(ns flambo.debug
  (:require [flambo.api :as f]
            [clojure.tools.logging :refer :all])
  )

(defn inspect [rdd name]
  (f/cache rdd)
  (f/rdd-name rdd name)
  (let [c (f/count rdd)]
    (info "#items@" name ": " c)
    (when-not (zero? c)
      (info "first items@" name ": " (f/first rdd))))
  rdd)