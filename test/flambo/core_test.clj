(ns flambo.core-test
  (:require [clojure.test :refer :all]
            [flambo.api :as f]))

;; StackOverFlow error unless Xss is increased or if we do
;; (vec (map + x y))
(deftest a-test
  (let [sc (f/local-spark-context :master "local" :job-name "optimization")
        data (-> (f/parallelize sc (repeat 10000 [1 2])) f/cache)]
    (is (= [10000 20000]
           (-> data (f/reduce (f/sparkop [x y] (map + x y))))))))
