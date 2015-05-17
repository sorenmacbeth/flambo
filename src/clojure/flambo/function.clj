(ns flambo.function
  (:require [serializable.fn :as sfn]
            [flambo.kryo :as kryo]
            [clojure.tools.logging :as log]
            [clojure.core.memoize :as memo]))

(set! *warn-on-reflection* true)

(defn- serfn? [f]
  (= (type f) :serializable.fn/serializable-fn))

(def serialize-fn sfn/serialize)


;; XXX: memoizing here is weird because all functions in a JVM now share a single
;;      cache lookup. Maybe we could memoize in the constructor or something instead?
;; TODO: what is a good cache size here???
(def deserialize-fn (memo/lru sfn/deserialize :lu/threshold 1000))
(def array-of-bytes-type (Class/forName "[B"))

;; ## Generic
(defn -init
  "Save the function f in state"
  [f]
  [[] f])

(defn -call [this & xs]
  (let [fn-or-serfn (.state this)
        f (if (instance? array-of-bytes-type fn-or-serfn)
            (binding [sfn/*deserialize* kryo/deserialize]
              (deserialize-fn fn-or-serfn))
            fn-or-serfn)]
    (log/trace "CLASS" (type this))
    (log/trace "META" (meta f))
    (log/trace "XS" xs)
    (log/trace (memo/snapshot deserialize-fn))
    (apply f xs)))

;; ## Functions
(defn mk-sym
  [fmt sym-name]
  (symbol (format fmt sym-name)))

(defmacro gen-function
  [clazz wrapper-name]
  (let [new-class-sym (mk-sym "flambo.function.%s" clazz)
        prefix-sym (mk-sym "%s-" clazz)]
    `(do
       (def ~(mk-sym "%s-init" clazz) -init)
       (def ~(mk-sym "%s-call" clazz) -call)
       (gen-class
        :name ~new-class-sym
        :implements [~(mk-sym "org.apache.spark.api.java.function.%s" clazz)]
        :prefix ~prefix-sym
        :init ~'init
        :state ~'state
        :constructors {[Object] []})
       (defn ~wrapper-name [f#]
         (new ~new-class-sym
              (if (serfn? f#)
                (binding [sfn/*serialize* kryo/serialize]
                  (serialize-fn f#)) f#))))))

(gen-function Function function)
(gen-function Function2 function2)
(gen-function Function3 function3)
(gen-function VoidFunction void-function)
(gen-function FlatMapFunction flat-map-function)
(gen-function FlatMapFunction2 flat-map-function2)
(gen-function PairFlatMapFunction pair-flat-map-function)
(gen-function PairFunction pair-function)
(gen-function DoubleFunction double-function)
(gen-function DoubleFlatMapFunction double-flat-map-function)

;; This sucks, but I need to do it to type hint the call to .state
;; and I don't think I can do that from the generic -call I used above.

(defn Function-call [^flambo.function.Function this & xs]
  (let [fn-or-serfn (.state this)
        f (if (instance? array-of-bytes-type fn-or-serfn)
            (binding [sfn/*deserialize* kryo/deserialize]
              (deserialize-fn fn-or-serfn))
            fn-or-serfn)]
    (log/trace "CLASS" (type this))
    (log/trace "META" (meta f))
    (log/trace "XS" xs)
    (log/trace (memo/snapshot deserialize-fn))
    (apply f xs)))

(defn Function2-call [^flambo.function.Function2 this & xs]
  (let [fn-or-serfn (.state this)
        f (if (instance? array-of-bytes-type fn-or-serfn)
            (binding [sfn/*deserialize* kryo/deserialize]
              (deserialize-fn fn-or-serfn))
            fn-or-serfn)]
    (log/trace "CLASS" (type this))
    (log/trace "META" (meta f))
    (log/trace "XS" xs)
    (log/trace "MEMO" (memo/snapshot deserialize-fn))
    (apply f xs)))

(defn Function3-call [^flambo.function.Function2 this & xs]
  (let [fn-or-serfn (.state this)
        f (if (instance? array-of-bytes-type fn-or-serfn)
            (binding [sfn/*deserialize* kryo/deserialize]
              (deserialize-fn fn-or-serfn))
            fn-or-serfn)]
    (log/trace "CLASS" (type this))
    (log/trace "META" (meta f))
    (log/trace "XS" xs)
    (log/trace "MEMO" (memo/snapshot deserialize-fn))
    (apply f xs)))

(defn VoidFunction-call [^flambo.function.VoidFunction this & xs]
  (let [fn-or-serfn (.state this)
        f (if (instance? array-of-bytes-type fn-or-serfn)
            (binding [sfn/*deserialize* kryo/deserialize]
              (deserialize-fn fn-or-serfn))
            fn-or-serfn)]
    (log/trace "CLASS" (type this))
    (log/trace "META" (meta f))
    (log/trace "XS" xs)
    (log/trace "MEMO" (memo/snapshot deserialize-fn))
    (apply f xs)))

(defn FlatMapFunction-call [^flambo.function.FlatMapFunction this & xs]
  (let [fn-or-serfn (.state this)
        f (if (instance? array-of-bytes-type fn-or-serfn)
            (binding [sfn/*deserialize* kryo/deserialize]
              (deserialize-fn fn-or-serfn))
            fn-or-serfn)]
    (log/trace "CLASS" (type this))
    (log/trace "META" (meta f))
    (log/trace "XS" xs)
    (log/trace "MEMO" (memo/snapshot deserialize-fn))
    (apply f xs)))

(defn FlatMapFunction2-call [^flambo.function.FlatMapFunction2 this & xs]
  (let [fn-or-serfn (.state this)
        f (if (instance? array-of-bytes-type fn-or-serfn)
            (binding [sfn/*deserialize* kryo/deserialize]
              (deserialize-fn fn-or-serfn))
            fn-or-serfn)]
    (log/trace "CLASS" (type this))
    (log/trace "META" (meta f))
    (log/trace "XS" xs)
    (log/trace "MEMO" (memo/snapshot deserialize-fn))
    (apply f xs)))

(defn PairFlatMapFunction-call [^flambo.function.PairFlatMapFunction this & xs]
  (let [fn-or-serfn (.state this)
        f (if (instance? array-of-bytes-type fn-or-serfn)
            (binding [sfn/*deserialize* kryo/deserialize]
              (deserialize-fn fn-or-serfn))
            fn-or-serfn)]
    (log/trace "CLASS" (type this))
    (log/trace "META" (meta f))
    (log/trace "XS" xs)
    (log/trace "MEMO" (memo/snapshot deserialize-fn))
    (apply f xs)))

(defn PairFunction-call [^flambo.function.PairFunction this & xs]
  (let [fn-or-serfn (.state this)
        f (if (instance? array-of-bytes-type fn-or-serfn)
            (binding [sfn/*deserialize* kryo/deserialize]
              (deserialize-fn fn-or-serfn))
            fn-or-serfn)]
    (log/trace "CLASS" (type this))
    (log/trace "META" (meta f))
    (log/trace "XS" xs)
    (log/trace "MEMO" (memo/snapshot deserialize-fn))
    (apply f xs)))

(defn DoubleFunction-call [^flambo.function.DoubleFunction this & xs]
  (let [fn-or-serfn (.state this)
        f (if (instance? array-of-bytes-type fn-or-serfn)
            (binding [sfn/*deserialize* kryo/deserialize]
              (deserialize-fn fn-or-serfn))
            fn-or-serfn)]
    (log/trace "CLASS" (type this))
    (log/trace "META" (meta f))
    (log/trace "XS" xs)
    (log/trace "MEMO" (memo/snapshot deserialize-fn))
    (apply f xs)))

(defn DoubleFlatMapFunction-call [^flambo.function.DoubleFlatMapFunction this & xs]
  (let [fn-or-serfn (.state this)
        f (if (instance? array-of-bytes-type fn-or-serfn)
            (binding [sfn/*deserialize* kryo/deserialize]
              (deserialize-fn fn-or-serfn))
            fn-or-serfn)]
    (log/trace "CLASS" (type this))
    (log/trace "META" (meta f))
    (log/trace "XS" xs)
    (log/trace "MEMO" (memo/snapshot deserialize-fn))
    (apply f xs)))
