(ns flambo.function
  (:require [serializable.fn :as sfn]
            [flambo.kryo :as kryo]
            [clojure.tools.logging :as log]))

(set! *warn-on-reflection* false)

(defn- serfn? [f]
  (= (type f) :serializable.fn/serializable-fn))

(def serialize-fn sfn/serialize)
(def deserialize-fn (memoize sfn/deserialize))
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
              (if (serfn? f#) (binding [sfn/*serialize* kryo/serialize]
                                (serialize-fn f#)) f#))))))

(gen-function Function function)
(gen-function Function2 function2)
(gen-function Function3 function3)
(gen-function VoidFunction void-function)
(gen-function FlatMapFunction flat-map-function)
(gen-function FlatMapFunction2 flat-map-function2)
(gen-function PairFlatMapFunction pair-flat-map-function)
(gen-function PairFunction pair-function)

;; Replaces the PairFunction-call and PairFlatMapFunction-call defined by the gen-function macro.
;; (defmulti PairFunction-call (fn [_ x] (class x)))

;; (defmethod PairFunction-call Tuple2
;;   [this x]
;;   (log/trace "PAIRFUNCTION-CALL TUPLE2")
;;   (-call this x))

;; (defmethod PairFunction-call :default
;;   [this x]
;;   (log/trace "PAIRFUNCTION-CALL DEFAULT")
;;   (let [[a b] (-call this x)]
;;     (Tuple2. a b)))

;; (defmulti PairFlatMapFunction-call (fn [_ x] (class x)))

;; (defmethod PairFlatMapFunction-call Tuple2
;;   [this x]
;;   (-call this x))

;; (defmethod PairFlatMapFunction-call :default
;;   [this x]
;;   (let [ret (-call this x)]
;;     (for [v ret
;;           :let [[a b] v]]
;;       (Tuple2. a b))))
