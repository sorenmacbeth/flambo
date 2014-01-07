(ns flambo.function
  (:require [flambo.utils :as u])
  (:import (flambo.function ClojureFlatMapFunction ClojureFunction
                            ClojureFunction2 ClojurePairFlatMapFunction
                            ClojurePairFunction)))

;; Function

(defn clojure-sparkfn* [fn-var args]
  (ClojureFunction. (u/to-spec fn-var) args))

(defmacro clojure-sparkfn [fn-sym args]
  `(clojure-sparkfn* (var ~fn-sym) ~args))

(defmacro sparkfn [& body]
  `(proxy [org.apache.spark.api.java.function.Function] []
     ~@body))

(defmacro defsparkfn [name & [opts & impl :as all]]
  (if-not (map? opts)
    `(defsparkfn ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          fn-body (let [[args & impl-body] impl
                        args (vec args)]
                    `(sparkfn (~'call ~args ~@impl-body)))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-sparkfn ~fn-name args#))
                    `(def ~name
                       (clojure-sparkfn ~fn-name [])))]
       `(do
         (defn ~fn-name ~(if params params [])
           ~fn-body)
         ~definer))))

;; Function2

(defn clojure-sparkfn2* [fn-var args]
  (ClojureFunction2. (u/to-spec fn-var) args))

(defmacro clojure-sparkfn2 [fn-sym args]
  `(clojure-sparkfn2* (var ~fn-sym) ~args))

(defmacro sparkfn2 [& body]
  `(proxy [org.apache.spark.api.java.function.Function2] []
     ~@body))

(defmacro defsparkfn2 [name & [opts & impl :as all]]
  (if-not (map? opts)
    `(defsparkfn2 ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          fn-body (let [[args & impl-body] impl
                        args (vec args)]
                    `(sparkfn2 (~'call ~args ~@impl-body)))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-sparkfn2 ~fn-name args#))
                    `(def ~name
                       (clojure-sparkfn2 ~fn-name [])))]
       `(do
         (defn ~fn-name ~(if params params [])
           ~fn-body)
         ~definer))))

;; PairFunction

(defn clojure-pairfn* [fn-var args]
  (ClojurePairFunction. (u/to-spec fn-var) args))

(defmacro clojure-pairfn [fn-sym args]
  `(clojure-pairfn* (var ~fn-sym) ~args))

(defmacro pairfn [& body]
  `(proxy [org.apache.spark.api.java.function.PairFunction] []
     ~@body))

(defmacro defpairfn [name & [opts & impl :as all]]
  (if-not (map? opts)
    `(defpairfn ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          fn-body (let [[args & impl-body] impl
                        args (vec args)]
                    `(pairfn (~'call ~args (u/tuple ~@impl-body))))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-pairfn ~fn-name args#))
                    `(def ~name
                       (clojure-pairfn ~fn-name [])))]
       `(do
         (defn ~fn-name ~(if params params [])
           ~fn-body)
         ~definer))))

;; FlatMapFunction

(defn clojure-flatmapfn* [fn-var args]
  (ClojureFlatMapFunction. (u/to-spec fn-var) args))

(defmacro clojure-flatmapfn [fn-sym args]
  `(clojure-flatmapfn* (var ~fn-sym) ~args))

(defmacro flatmapfn [& body]
  `(proxy [org.apache.spark.api.java.function.FlatMapFunction] []
     ~@body))

(defmacro defflatmapfn [name & [opts & impl :as all]]
  (if-not (map? opts)
    `(defflatmapfn ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          fn-body (let [[args & impl-body] impl
                        args (vec args)]
                    `(flatmapfn (~'call ~args ~@impl-body)))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-flatmapfn ~fn-name args#))
                    `(def ~name
                       (clojure-flatmapfn ~fn-name [])))]
       `(do
         (defn ~fn-name ~(if params params [])
           ~fn-body)
         ~definer))))

;; PairFlatMapFunction

(defn clojure-pflatmapfn* [fn-var args]
  (ClojurePairFlatMapFunction. (u/to-spec fn-var) args))

(defmacro clojure-pflatmapfn [fn-sym args]
  `(clojure-pflatmapfn* (var ~fn-sym) ~args))

(defmacro pflatmapfn [& body]
  `(proxy [org.apache.spark.api.java.function.PairFlatMapFunction] []
     ~@body))

(defmacro defpflatmapfn [name & [opts & impl :as all]]
  (if-not (map? opts)
    `(defpflatmapfn ~name {} ~@all)
    (let [params (:params opts)
          fn-name (symbol (str name "__"))
          fn-body (let [[args & impl-body] impl
                        args (vec args)]
                    `(pflatmapfn (~'call ~args ~@impl-body)))
          definer (if params
                    `(defn ~name [& args#]
                       (clojure-pflatmapfn ~fn-name args#))
                    `(def ~name
                       (clojure-pflatmapfn ~fn-name [])))]
       `(do
         (defn ~fn-name ~(if params params [])
           ~fn-body)
         ~definer))))
