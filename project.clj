(defproject yieldbot/flambo "0.5.0-SNAPSHOT"
  :description "A Clojure DSL for Apache Spark"
  :url "https://github.com/yieldbot/flambo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :mailing-list {:name "flambo user mailing list"
                 :archive "https://groups.google.com/d/forum/flambo-user"
                 :post "flambo-user@googlegroups.com"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [yieldbot/serializable-fn "0.0.6"
                  :exclusions [com.twitter/chill-java]]
                 [com.twitter/carbonite "1.4.0"
                  :exclusions [com.twitter/chill-java]]
                 [com.twitter/chill_2.10 "0.3.6"
                  :exclusions [org.scala-lang/scala-library]]]
  :profiles {:dev
             {:dependencies [[midje "1.6.3"]
                             [criterium "0.4.3"]]
              :plugins [[lein-midje "3.1.3"]
                        [lein-marginalia "0.8.0"]
                        [codox "0.8.9"]]
              ;; so gen-class stuff works in the repl
              :aot [flambo.function
                    flambo.example.tfidf]}
             :provided
             {:dependencies
              [[org.apache.spark/spark-core_2.10 "1.2.0"]
               [org.apache.spark/spark-streaming_2.10 "1.2.0"]
               [org.apache.spark/spark-streaming-kafka_2.10 "1.2.0"]
               [org.apache.spark/spark-streaming-flume_2.10 "1.2.0"]
               [org.apache.spark/spark-sql_2.10 "1.2.0"]]}
             :clojure-1.7
             {:dependencies [[org.clojure/clojure "1.7.0-alpha4"]]}
             :uberjar
             {:aot :all}
             :example
             {:main flambo.example.tfidf
              :source-paths ["test/flambo/example"]
              :aot [flambo.example.tfidf]}}
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :codox {:defaults {:doc/format :markdown}
          :include [flambo.api flambo.conf flambo.kryo]
          :output-dir "doc/codox"
          :src-dir-uri "http://github.com/yieldbot/flambo/blob/develop/"
          :src-linenum-anchor-prefix "L"}
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :jvm-opts ^:replace ["-server" "-Xmx1g"]
  :global-vars {*warn-on-reflection* false}
  :min-lein-version "2.5.0")
