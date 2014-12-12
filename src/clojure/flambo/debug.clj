(ns flambo.debug
  (:require [flambo.api :as f]
            [clojure.tools.logging :as log]))

(defn inspect [rdd name]
  (let [cached (-> rdd
                   (f/cache)
                   (f/rdd-name name))]
    (try
      (log/info name "partition count:" (f/partition-count cached))
      (log/info name "partitioner:" (f/partitioner cached))
      (catch Throwable t))
    (let [c (f/count cached)]
      (log/info name "item count:" c)
      (when-not (zero? c)
        (log/info name "first item:" (f/first cached))))
    cached))
