(ns flambo.function
  (:require [serializable.fn :as sfn]
            [flambo.kryo :as kryo]
            [flambo.cache :as cache]))

(set! *warn-on-reflection* true)

(defn- serfn? [f]
  (= (type f) :serializable.fn/serializable-fn))

(def serialize-fn sfn/serialize)


;; XXX: memoizing here is weird because all functions in a JVM now share a single
;;      cache lookup. Maybe we could memoize in the constructor or something instead?
;; TODO: what is a good cache size here???
(def deserialize-fn (cache/lru-memoize 5000 sfn/deserialize))
(def array-of-bytes-type (Class/forName "[B"))

;; ## Generic
(defn -init
  "Save the function f in state"
  [f]
  [[] f])

;; ## Functions
(defn mk-sym
  [fmt sym-name]
  (symbol (format fmt sym-name)))

(defmacro gen-function
  [clazz wrapper-name]
  (let [new-class-sym (mk-sym "flambo.function.%s" clazz)
        prefix-sym (mk-sym "%s-" clazz)]
    `(do
       (gen-class
        :name ~new-class-sym
        :extends flambo.function.AbstractFlamboFunction
        :implements [~(mk-sym "org.apache.spark.api.java.function.%s" clazz)]
        :prefix ~prefix-sym
        :init ~'init
        :state ~'state
        :constructors {[Object] []})
       (def ~(mk-sym "%s-init" clazz) -init)
       (defn ~(mk-sym "%s-call" clazz)
         [~(vary-meta 'this assoc :tag new-class-sym) & ~'xs]
         (let [fn-or-serfn# (.state ~'this)
               f# (if (instance? array-of-bytes-type fn-or-serfn#)
                    (binding [sfn/*deserialize* kryo/deserialize]
                      (deserialize-fn fn-or-serfn#))
                    fn-or-serfn#)]
           (apply f# ~'xs)))
       (defn ~wrapper-name [f#]
         (new ~new-class-sym
              (if (serfn? f#)
                (binding [sfn/*serialize* kryo/serialize]
                  (serialize-fn f#)) f#))))))

(gen-function Function function)
(gen-function Function2 function2)
(gen-function Function3 function3)
(gen-function VoidFunction void-function)
(gen-function VoidFunction2 void-function2)
(gen-function FlatMapFunction flat-map-function)
(gen-function FlatMapFunction2 flat-map-function2)
(gen-function PairFlatMapFunction pair-flat-map-function)
(gen-function PairFunction pair-function)
(gen-function DoubleFunction double-function)
(gen-function DoubleFlatMapFunction double-flat-map-function)

