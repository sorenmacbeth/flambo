(ns flambo.sql-test
  (:use midje.sweet)
  (:require [flambo.api   :as f]
            [flambo.tuple :as ft]
            [flambo.conf  :as conf]
            [flambo.sql   :as sql]))

(facts
 "about spark-sql-context"
 (let [conf (-> (conf/spark-conf)
                (conf/master "local[*]")
                (conf/app-name "sql-test"))]
   (sql/with-sql-context c conf
     (fact
      "gives us a JavaSQLContext"
      (class c) => org.apache.spark.sql.SQLContext)

     (fact
      "gives us a DataFrame"
      (class (sql/load c "test/resources/data.csv" "com.databricks.spark.csv")) => org.apache.spark.sql.DataFrame)

     (fact
      "returns an array of column names from a CSV file"
      (let [df (sql/read-csv c "test/resources/cars.csv" :header true)]
        (sql/columns df) => ["year" "make" "model" "comment" "blank"]))

     
     )))
