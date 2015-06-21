(ns flambo.accumulator
  (:import [org.apache.spark Accumulator]
           [scala.Option])
  (:refer-clojure :exclude [name]))

(defn accumulator
  ([sc value]
   (.accumulator sc value))
  ([sc value name]
   (.accumulator sc value name )))

(defn value [^Accumulator accumulator-var]
  (.value accumulator-var))

(defn name [^Accumulator accumulator-var]
  (let [name-var (.name accumulator-var)]
    (if (= scala.Some (class name-var))
      (.x name-var))))
