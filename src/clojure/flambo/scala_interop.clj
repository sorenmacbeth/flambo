(ns flambo.scala-interop
  (:require [serializable.fn :as sfn])
  (:import [scala Tuple2 Some]))

(defn some-or-nil [option]
  (when (instance? Some option)
    (.get option)))
