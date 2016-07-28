(ns flambo.broadcast-test
  (:use midje.sweet)
  (:require [flambo.api :as f]
            [flambo.conf :as conf]
            [flambo.broadcast :as b]))


(facts
 "about broadcast"
 (let [conf (-> (conf/spark-conf)
                (conf/master "local[*]")
                (conf/app-name "broadcast-test"))]
   (f/with-context c conf
     (let [bc (b/broadcast c {:one "foo"})]
       (-> (f/parallelize c [1 2 3 4 5])
           (f/foreach
            (f/fn [x]
              (fact "foo" (b/value bc) => {:one "foo"}))))))))
