(ns flambo.scalaInterop
  (:require [serializable.fn :as sfn]
            [flambo.utils :as u]
            [flambo.kryo :as kryo]
            [clojure.tools.logging :as log])
  (:import [scala Function0 Function1]
           [java.io Serializable]))

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

(defn -apply [this & xs]
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
  (let [new-class-sym (mk-sym "flambo.scalaInterop.%s" clazz)
        prefix-sym (mk-sym "%s-" clazz)]
    `(do
       (def ~(mk-sym "%s-init" clazz) -init)
       (def ~(mk-sym "%s-apply" clazz) -apply)
       (gen-class
         :name ~new-class-sym
         :implements [~(mk-sym "scala.%s" clazz) Serializable]
         :prefix ~prefix-sym
         :init ~'init
         :state ~'state
         :constructors {[Object] []})
       (defn ~wrapper-name [f#]
         (new ~new-class-sym
              (if (serfn? f#) (binding [sfn/*serialize* kryo/serialize]
                                (serialize-fn f#)) f#))))))


(gen-function Function0 function0)
(gen-function Function1 function1)


#_(defn Function0-apply [this]
  (-apply this))

#_(defn Function1-apply [this x]
  (-apply this x))

