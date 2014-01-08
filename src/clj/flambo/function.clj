(ns flambo.function
  (:require [serializable.fn :as sfn])
  (:import [scala Tuple2]))

(defn- serfn? [f]
  (= (type f) :serializable.fn/serializable-fn))

(def serialize-fn sfn/serialize)
(def deserialize-fn (memoize sfn/deserialize))
(def array-of-bytes-type (Class/forName "[B"))

;;; Generic

(defn -init
  "Save the function f in state"
  [f]
  [[] f])

(defn -call [this & xs]
  (let [fn-or-serfn (.state this)
        f (if (instance? array-of-bytes-type fn-or-serfn)
            (deserialize-fn fn-or-serfn)
            fn-or-serfn)]
    (apply f xs)))

;;; Functions

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
        :extends ~(mk-sym "org.apache.spark.api.java.function.%s" clazz)
        :prefix ~prefix-sym
        :init ~'init
        :state ~'state
        :constructors {[Object] []})
       (defn ~wrapper-name [f#]
         (new ~new-class-sym
              (if (serfn? f#) (serialize-fn f#) f#))))))

(gen-function Function function)
(gen-function Function2 function2)
(gen-function VoidFunction void-function)
(gen-function FlatMapFunction flat-map-function)
(gen-function PairFunction pair-function)

;; Replaces the PairFunction-call defined by the gen-function macro.
(defn PairFunction-call [this x]
  (let [[a b] (-call this x)]
    (Tuple2. a b)))
