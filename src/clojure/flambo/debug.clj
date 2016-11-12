(ns flambo.debug
  (:require [flambo.api :as f]
            [clojure.tools.logging :as log]))

(defn inspect [rdd name & {:keys [count? cache?]
                           :or {count? false
                                cache? false}}]
  (let [rdd (if cache?
              (-> rdd
                  (f/cache)
                  (f/rdd-name name))
              rdd)]
    (try
      (log/info name "partition count:" (f/partition-count rdd))
      (log/info name "partitioner:" (f/partitioner rdd))
      (catch Exception e))
    (when count?
      (let [c (f/count rdd)]
        (log/info name "item count:" c)))
    (log/info name "first item:" (f/first rdd))
    rdd))
