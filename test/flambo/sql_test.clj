(ns flambo.sql-test
  (:use midje.sweet)
  (:require [flambo.api   :as f]
            [flambo.conf  :as conf]
            [flambo.sql   :as sql]
            [flambo.function :as func])
  (:import [org.apache.spark.sql functions RelationalGroupedDataset Column]
           [org.apache.spark.sql.expressions WindowSpec]
           [org.apache.spark.sql.types DataTypes IntegerType StringType]        
           ))

(facts
 "about spark-sql-context"
 (let [conf (-> (conf/spark-conf)
                (conf/master "local[*]")
                (conf/app-name "sql-test"))]
   (sql/with-sql-context c conf
     (let [sc (sql/spark-context c)
           test-data ["{\"col1\":4,\"col2\":\"a\"}" "{\"col1\":6,\"col2\":\"a\"}" "{\"col1\":5,\"col2\":\"b\"}"]
           test-df (sql/json-rdd c (f/parallelize sc test-data))
           test-data-2 ["{\"col1\":4,\"col2\":\"a\"}" "{\"col1\":4,\"col2\":\"a\"}" "{\"col1\":6,\"col2\":\"a\"}"]
           test-df-2 (sql/json-rdd c (f/parallelize sc test-data-2))
           _ (sql/register-data-frame-as-table c test-df "foo")
           _ (sql/register-data-frame-as-table c test-df-2 "bar")
           _ (-> c .udf (.register "UC" (func/udf (f/fn [x] (.toUpperCase x))) DataTypes/StringType))]
       (fact
         "with-sql-context gives us a SQLContext"
         (class c) => org.apache.spark.sql.SQLContext)

       (fact
         "load gives us a DataFrame"
         (class (sql/load c "test/resources/data.csv" "com.databricks.spark.csv")) => org.apache.spark.sql.Dataset)

       (fact
         "returns an array of column names from a CSV file"
         (let [df (sql/read-csv c "test/resources/cars.csv" :header true)]
           (sql/columns df) => ["year" "make" "model" "comment" "blank"]))

       (fact "SQL queries work"
           (f/count (sql/sql c "SELECT * FROM foo WHERE col2 = 'a'")) => 2)

       (fact "SQL UDF queries work"
          (f/count (sql/sql c "SELECT * FROM foo WHERE UC(col2) = 'A'")) => 2)

       (fact "table-names gets all tables"
         (sql/table-names c) => (just ["foo" "bar"] :in-any-order))

       (fact "table returns dataframe with the data for given name"
         (f/first (sql/table c "foo")) => (f/first test-df)
         (f/count (sql/table c "foo")) => (f/count test-df))

       (fact "print schema displays a the dataframe schema"
         (sql/print-schema test-df))

       (fact "cache table puts a given table into the cache"
         (let [_ (sql/cache-table c "foo")]
           (sql/is-cached? c "foo")) => true)

       (fact "uncache table removes a table from the cache"
         (let [_ (sql/cache-table c "bar")
               _ (sql/uncache-table c "bar")]
           (sql/is-cached? c "bar")) => false)

       (fact "clear-cache removes all tables from the cache"
         (let [_ (sql/cache-table c "foo")
               _ (sql/cache-table c "bar")
               _ (sql/clear-cache c)]
           (or (sql/is-cached? c "foo") (sql/is-cached? c "bar"))) => false)

       (fact "select returns a DataFrame"
             (class (sql/select test-df "*")) => org.apache.spark.sql.Dataset
             (class (sql/select test-df "col1" "col2")) => org.apache.spark.sql.Dataset)

       (fact "select returns expected columns"
          (sql/columns (sql/select test-df "*")) => ["col1" "col2"]
          (sql/columns (sql/select test-df "col1")) => ["col1"]
          (sql/columns (sql/select test-df "col2")) => ["col2"])

       (fact "where returns expected number of rows"
          (.count (sql/where test-df "1 = 1")) => 3
          (.count (sql/where test-df "1 = 0 ")) => 0
          (.count (sql/where test-df "col1 > 4")) => 2)

       (fact "group-by returns GroupedData object"
             (class (sql/group-by test-df)) => RelationalGroupedDataset)

       (fact "agg on grouped data returns a DataFrame"
             (class (sql/agg (sql/group-by test-df 'col2) (functions/sum (functions/lit 1)))) => org.apache.spark.sql.Dataset)

       (fact "window returns WindowSpec"
             (class (sql/window)) => WindowSpec)

       (fact "order-by returns WindowSpec"
             (class (sql/order-by (sql/window))) => WindowSpec)

       (fact "partition-by returns WindowSpec"
             (class (sql/partition-by (sql/window))) => WindowSpec)

       (fact "rows-between returns WindowSpec"
             (class (sql/rows-between (sql/window) -1 0)) => WindowSpec)

       (fact "ramge-between returns WindowSpec"
             (class (sql/range-between (sql/window) -1 0)) => WindowSpec)

       (fact "over returns a column"
             (class (sql/over (functions/sum "foo") (sql/window))) => Column)

       (fact "hive-context returns a HiveContext"
             (class (sql/hive-context sc)) => org.apache.spark.sql.hive.HiveContext)       
       )
     (let [
           schema (sql/create-custom-schema
                   [["id" DataTypes/IntegerType true]
                    ["name" DataTypes/StringType true]
                    ["seq" DataTypes/IntegerType true]])
           df (sql/read-csv c "test/resources/no-header.csv" :header false :schema schema)
           ]
       (fact
        "returns an array of column names from a CSV file using custom schema"
        (sql/columns df) => ["id" "name" "seq"])
       (fact
        "count test"
        (sql/count df) => 3)       
       )
     )
   
   
   )
 )



