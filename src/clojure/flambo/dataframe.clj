(ns ^{:doc "Functions to map, reduce, filter etc just like we do for RDDs."
      :author "Paul Landes"}
    flambo.dataframe
  (:refer-clojure :exclude [map filter reduce])
  (:import [org.apache.spark.sql Encoders])
  (:require [flambo.function :refer [map-function
                                     filter-function
                                     reduce-function
                                     flat-map-function]]))

(defn encoder-for-type
  "Create a Spark SQL Dataframe for **type**.  The available options for
  **type** are:

  * :object
  * :string
  * :boolean
  * :byte
  * :java.sql.Date
  * :java.math.BigDecimal
  * :double
  * :float
  * :integer
  * :long
  * :string-tuple"
  [type]
  (cond (keyword? type)
        (case type
          :object (Encoders/javaSerialization java.io.Serializable)
          :string (Encoders/STRING)
          :boolean (Encoders/BOOLEAN)
          :byte (Encoders/BYTE)
          :java.sql.Date (Encoders/DATE)
          :java.math.BigDecimal (Encoders/DECIMAL)
          :double (Encoders/DOUBLE)
          :float (Encoders/FLOAT)
          :integer (Encoders/INT)
          :long (Encoders/LONG)
          :string-tuple (Encoders/tuple (Encoders/STRING) (Encoders/STRING)))
        true type))

(defn map
  "Returns a new dataframe formed by passing each element of the source through the function `f`."
  ([df f] (map df :object f))
  ([df type f]
   (->> (encoder-for-type type)
        (.map df (map-function f)))))

(defn filter
  "Returns a new dataframe containing only the elements of `df` that satisfy a predicate `f`."
  [df f]
  (.filter df (filter-function f)))

(defn reduce
  "Returns a new dataframe containing only the elements of `df` that satisfy a predicate `f`."
  [df f]
  (.reduce df (reduce-function f)))

(defn flat-map
  "Similar to `map`, but each input item can be mapped to 0 or more output items (so the
  function `f` should return a collection rather than a single item)"
  ([df f] (flat-map df :object f))
  ([df type f]
   (->> (encoder-for-type type)
        (.flatMap df (flat-map-function f)))))
