;;
;; SparkSQL & DataFrame wrapper
;;
(ns flambo.sql
  (:refer-clojure :exclude [load group-by partition-by count map filter reduce])

  (:require [flambo.api :as f :refer [defsparkfn]]
            [flambo.sql-functions :as sqlf]
            [flambo.function :refer [map-function
                                     filter-function
                                     reduce-function
                                     flat-map-function]])

      (:import [org.apache.spark.api.java JavaSparkContext]
           [org.apache.spark.sql.types StructType StructField DataType StringType]
           [org.apache.spark.sql SparkSession SQLContext Row RowFactory Dataset Column]
           [org.apache.spark.sql.catalyst.expressions GenericRowWithSchema]
           [org.apache.spark.sql.hive HiveContext]
           [org.apache.spark.sql.expressions Window]
           [org.apache.spark.sql.types DataTypes]
           [org.apache.spark.sql Encoders]))

;; ## SQLContext

(defn ^SQLContext sql-context
  "Build a SQLContext from a JavaSparkContext"
  [^JavaSparkContext spark-context]
  (SQLContext. spark-context))

(defn ^SQLContext hive-context
  "Build a HiveContext from a JavaSparkContext"
  [^JavaSparkContext spark-context]
  (HiveContext. spark-context))

(defn ^JavaSparkContext spark-context
  "Get reference to the SparkContext out of a SQLContext"
  [^SQLContext sql-context]
  (JavaSparkContext/fromSparkContext (.sparkContext sql-context)))

(defn sql
  "Execute a query. The dialect that is used for SQL parsing can be configured with 'spark.sql.dialect'."
  [sql-context query]
  (.sql sql-context query))

(defmacro with-sql-context
  [context-sym conf & body]
  `(let [~'x (f/spark-context ~conf)
         ~context-sym (sql-context ~'x)]
     (try
       ~@body
       (finally (.stop ~'x)))))

(defn parquet-file
  "Loads a Parquet file, returning the result as a Dataset."
  [sql-context path]
  (.parquetFile sql-context path))

(defn json-file
  "Loads a JSON file (one object per line), returning the result as a DataFrame."
  [sql-context path]
  (-> sql-context .read (.format "json") (.load path)))

;; Since 1.3 the SparkSQL data sources API is recommended for load & save operations.
(defn load
  "Returns the dataset stored at path as a Dataset."
  ([sql-context path]                   ; data source type configured by spark.sql.sources.default
   (.load sql-context path))
  ([sql-context path source-type]       ; specify data source type
   (.load sql-context path source-type)))

(defn create-custom-schema [array]
  (-> (clojure.core/map #(DataTypes/createStructField (first %) (second %) (nth % 2))  array)
      DataTypes/createStructType))

(defn read-csv
  "Reads a file in table format and creates a data frame from it, with cases corresponding to
  lines and variables to fields in the file. A clone of R's read.csv."
  [sql-context path &{:keys [header delimiter quote schema]
                      :or   {header false delimiter "," quote "'"}}]
  (let [options (new java.util.HashMap)]
    (.put options "path" path)
    (.put options "header" (if header "true" "false"))
    (.put options "delimiter" delimiter)
    (.put options "quote" quote)
    (if schema
      (-> sql-context .read (.format "csv") (.options options) (.schema schema) .load)
      (-> sql-context .read (.format "csv") (.options options) .load)
      )))


(defn register-data-frame-as-table
  "Registers the given DataFrame as a temporary table in the
  catalog. Temporary tables exist only during the lifetime of this
  instance of SQLContex."
  [sql-context df table-name]
  (.registerDataFrameAsTable sql-context df table-name))

(defn cache-table
  "Caches the specified table in memory."
  [sql-context table-name]
  (.cacheTable sql-context table-name))

(defn json-rdd
  "Load an RDD of JSON strings (one object per line), inferring the schema, and returning a DataFrame"
  [sql-context json-rdd]
  (-> sql-context .read (.json json-rdd)))

(defn uncache-table
  "Removes the specified table from the in-memory cache."
  [sql-context table-name]
  (.uncacheTable sql-context table-name))

(defn clear-cache
  "Remove all tables from cache"
  [sql-context]
  (.clearCache sql-context))

(defn is-cached?
  "Is the given table cached"
  [sql-context table-name]
  (.isCached sql-context table-name))

(defn ^Dataset table
  "Return a table as a DataFrame"
  [sql-context table-name]
  (.table sql-context table-name))

(defn table-names
  "Return a seq of strings of table names, optionally within a specific database"
  ([sql-context]
   (seq (.tableNames sql-context)))
  ([sql-context database-name]
   (seq (.tableNames sql-context database-name))))

(defn- as-col-array
  [exprs]
  (into-array Column (clojure.core/map sqlf/col exprs)))

(defn select
  "Select a set of columns"
  [df & exprs]
  (.select df (as-col-array exprs)))

(defn where
  "Filters DataFrame rows using SQL expression"
  [df expr]
  (.where df expr))

(defn group-by
  "Groups data using the specified expressions"
  [df & exprs]
  (.groupBy df (as-col-array exprs)))

(defn agg
  "Aggregates grouped data using the specified expressions"
  [df expr & exprs]
  (.agg df (sqlf/col expr) (as-col-array exprs)))

;; DataFrame
(defn register-temp-table
  "Registers this dataframe as a temporary table using the given name."
  [df table-name]
  (.registerTempTable df table-name))

(defn columns
  "Returns all column names as a sequence."
  [df]
  (seq (.columns df)))

(def print-schema (memfn printSchema))

(defn window
  "Create an empty window specification"
  []
  (Window/partitionBy (as-col-array [])))

(defn order-by
  "Create window spec with specified ordering"
  [w & exprs]
  (.orderBy w (as-col-array exprs)))

(defn partition-by
  "Create window spec with specified partitioning"
  [w & exprs]
  (.partitionBy w (as-col-array exprs)))

(defn rows-between
  "Create window spec with row window specified"
  [w lower-bound upper-bound]
  (.rowsBetween w lower-bound upper-bound))

(defn range-between
  "Create window spec with range window specified"
    [w lower-bound upper-bound]
    (.rangeBetween w lower-bound upper-bound))

(defn over
  "Return expresion with a given windowing"
  [exprs w]
  (.over (sqlf/col exprs) w))

;; Row
(defsparkfn row->vec [^Row row]
  (let [n (.length row)]
    (loop [i 0 v (transient [])]
      (if (< i n)
        (recur (inc i) (conj! v (.get row i)))
        (persistent! v)))))

(defsparkfn
  ^{:doc "Coerce `org.apache.spark.sql SparkSession.Row` objects into Clojure
maps with each map created from its respective row."}
  row->map [^org.apache.spark.sql.Row row]
  (let [n (.length row)
        fields (-> row .schema .fieldNames)]
    (loop [i 0 m (transient {})]
      (if (< i n)
        (recur (inc i) (assoc! m (keyword (nth fields i)) (.get row i)))
        (persistent! m)))))

(defsparkfn
  ^{:doc "Coerce an Scala interator into a Clojure sequence"}
  iteratable-to-seq [i]
  (-> (.iterator i)
      scala.collection.JavaConversions/asJavaIterator
      iterator-seq))

(defsparkfn
  ^{:doc "Coerce a Spark SQL StructField to a Clojure map."}
  struct-to-map [^StructField struct]
  {:name (.name struct)
   :type (->> (.dataType struct) .toString (re-find #"^(.*)Type$") last)
   :nullable (.nullable struct)})

(defsparkfn ^{:doc "Coerce a Spark Dataset to a schema map with [[struct-to-map]]"}
  schema [^org.apache.spark.sql.Dataset dataset]
  (-> dataset
      f/to-rdd
      (f/map (f/fn [row]
               (.schema row)))
      f/first
      iteratable-to-seq
      (#(clojure.core/map struct-to-map %))))

(defn setup-temp-view
  "Create or replace a temporary view from a JSON file or Parquet at **url**.
  The temporary name will be set to **table-name** (if provided) and the type
  of data is expected is given in the `:type` key, which is one of the
  following:

* `:parquet` - parquet table format (default)
* `:json` - a JSON formatted file
* `:csv` - a comma delimited file"
  [url table-name & {:keys [type csv-options schema infer-schema?]
                     :or {infer-schema? true
                          csv-options {:header true :separator "," :quote "'"}}}]
  (let [type (or type :parquet)
        options (java.util.HashMap.)
        infer-schema? (and (nil? schema) infer-schema?)]
    (.put options "path" url)
    (if (and (= type :csv) csv-options)
      (->> (zipmap (clojure.core/map name (keys csv-options))
                   (clojure.core/map str (vals csv-options)))
          (.putAll options)))
    (if (and (nil? schema) infer-schema?)
      (.put options "inferSchema" "true"))
    (-> (sql-context)
        .read
        (.format (name type))
        (.options options)
        ((fn [df]
           (if infer-schema?
             (.schema df schema))
           df))
        .load
        ((fn [df]
           (if table-name
             (.createOrReplaceTempView df table-name)
             df))))))

(defn query
  "Query a parquet file at **url** with **sql** statement creating temporary
  table key **table** (default to `tmp`).

  See [[setup-temp-view]] for `:type` key."
  [url sql & {:keys [table partitions] :as m
              :or {table "tmp"}}]
  (apply setup-temp-view url table (apply concat m))
  (-> (.getOrCreate (SparkSession/builder))
      (.sql sql)
      ((fn [df]
         (if partitions
           (.repartition df partitions)
           df)))))

(defn schema-from-url
  "Return a schema as a map from a parquet table at **url**.

  See [[setup-temp-view]] for `:type` key."
  [url & {:keys [type]}]
  (-> (query url "select * from tmp" :type type)
      (schema)))

(defn struct-type
  "Create a `org.apache.spark.sql.types.StructType` from an array of maps, each
  having the following keys:

* **:name** the name of the column
* **:type** the (keyword) type of the column; defaults to `:string`
* **:nullable** boolean of whether the column is nullable; defaults to `true`"
  [defs]
  (let [metadata (org.apache.spark.sql.types.Metadata/empty)]
    (->> defs
         (clojure.core/map
          (fn [{:keys [name type nullable?]
                :or {type :string
                     nullable? true}}]
            (->> type
                 clojure.core/name
                 (format "\"%s\"")
                 DataType/fromJson
                 (#(StructField. name % nullable? metadata)))))
         (into-array StructField)
         StructType.)))

(defn create-row
  "Create a `org.apache.spark.sql SparkSession.Row` instance from a Clojure
  sequence.  If **schema** is given, add the row with a schema."
  ([seq]
   (RowFactory/create (into-array Object seq)))
  ([seq schema]
   (GenericRowWithSchema. (into-array Object seq) schema)))

(defn query-vecs
  "Query and return a sequence of vectors (each containing a row) from a
  parquet table at **url** with **sql**.

See [[query]] for **opts** details."
  [url sql & opts]
  (-> (apply query url sql opts)
      f/to-rdd
      (f/map row->vec)))

(defn query-maps
  "Query and return a sequence of maps (each containing a row) from a
  parquet table at **url** with **sql**.

See [[query]] for **opts** details."
  [url sql & opts]
  (-> (apply query url sql opts)
      f/to-rdd
      (f/map row->map)))

(defn print-query
  "Print a query as a tabulated text format.

  See [[query-maps]] for information on parameters and keys **opts**."
  [url sql & opts]
  (->> (apply query-maps url sql opts)
       f/collect
       clojure.pprint/print-table))


(def show (memfn show))

(def count (memfn count))

(def create-global-temp-view (memfn createGlobalTempView))

(defn create-or-replace-temp-view [v-name]
  (.createOrReplaceTempView v-name))

(defn encoder-for-type
  "Create a Spark SQL Encoder for **type**.  The available options for
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
  (cond (instance? java.lang.Class type) (Encoders/javaSerialization type)
        (keyword? type)
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
  ([df f] (clojure.core/map df :object f))
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
