(ns flambo.rdd.jdbc-test
  (:import [java.sql Connection PreparedStatement ResultSet DatabaseMetaData ResultSetMetaData])
  (:require [flambo.rdd.jdbc :refer :all]
            [flambo.api :as f]
            [flambo.conf :as conf]
            [midje.sweet :refer :all]))

(let [data (atom [nil 1 2 3 4 5 6 7 8 9 10])] ;; have something before the first element to make it easier to handle the first call to "next".
  (f/defsparkfn get-connection []
                (proxy [Connection]
                       []
                  (prepareStatement [sql resultSetType resultSetConcurrency]

                    (proxy [PreparedStatement] []
                      (executeQuery []
                        (proxy [ResultSet] []
                          (isClosed [] false)
                          (close [])
                          (next [] (do (swap! data rest)
                                       (not (empty? @data))))
                          (getMetaData []
                            (proxy [ResultSetMetaData] []
                              (getColumnCount [] 1)
                              (getColumnLabel [index] "id-column")
                              ))
                          (getObject [index]
                            (first @data)
                            )))
                      (setLong [parameterIndex, x])
                      (isClosed [] false)
                      (close [])))
                  (isClosed [] false)
                  (close [])
                  (getMetaData []
                    (proxy [DatabaseMetaData] []
                      (getURL [] ""))))))

(facts
  "about other stuff"

  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name "jdbc-test"))]
    (f/with-context c conf
                    (fact
                      "load stuff from jdbc"
                      (f/collect (load-jdbc c get-connection "query" 0 10 1))
                      => [{:id-column 1} {:id-column 2} {:id-column 3} {:id-column 4} {:id-column 5} {:id-column 6} {:id-column 7} {:id-column 8} {:id-column 9} {:id-column 10}]
                      )
                    )))