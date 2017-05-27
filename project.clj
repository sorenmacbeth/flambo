(defproject yieldbot/flambo "0.8.1-SNAPSHOT"
  :description "A Clojure DSL for Apache Spark"
  :url "https://github.com/yieldbot/flambo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :mailing-list {:name "flambo user mailing list"
                 :archive "https://groups.google.com/d/forum/flambo-user"
                 :post "flambo-user@googlegroups.com"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.google.guava/guava "18.0"]
                 [yieldbot/serializable-fn "0.1.2"
                  :exclusions [com.twitter/chill-java]]
                 [com.twitter/carbonite "1.5.0"
                  :exclusions [com.twitter/chill-java]]
                 [com.twitter/chill_2.11 "0.8.0"
                  :exclusions [org.scala-lang/scala-library]]]
  :aot       :all
  :profiles {:dev
             {:dependencies [[midje "1.6.3"]
                             [criterium "0.4.3"]]
              :plugins [[lein-midje "3.1.3"]
                        [michaelblume/lein-marginalia "0.9.0"]
                        ;; [codox "0.8.9"]
                        [funcool/codeina "0.3.0"
                         :exclusions [org.clojure/clojure]]]
              ;; so gen-class stuff works in the repl
              :aot [flambo.function
                    flambo.example.tfidf]}
             :provided
             {:dependencies
              [[org.apache.spark/spark-core_2.11 "2.1.0"]
               [org.apache.spark/spark-streaming_2.11 "2.1.0"]
               [org.apache.spark/spark-streaming-kafka-0-8_2.11 "2.1.0"]
               [org.apache.spark/spark-sql_2.11 "2.1.0"]
               [org.apache.spark/spark-hive_2.11 "2.1.0"]]}
             :clojure-1.6
             {:dependencies [[org.clojure/clojure "1.6.0"]]}
             :uberjar
             {:aot :all}
             :example
             {:main flambo.example.tfidf
              :source-paths ["test/flambo/example"]
              :aot [flambo.example.tfidf]}}
  :checksum :warn ;; https://issues.apache.org/jira/browse/SPARK-5308
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :codeina {:reader :clojure
            :src ["src/clj"]
            :target "doc/codeina"
            :src-uri "https://github.com/yieldbot/flambo/blob/develop/"
            :src-uri-prefix "#L"
            }
  :codox {:defaults {:doc/format :markdown}
          :include [flambo.api flambo.conf flambo.kryo flambo.sql]
          :output-dir "doc/codox"
          :src-dir-uri "http://github.com/yieldbot/flambo/blob/develop/"
          :src-linenum-anchor-prefix "L"}
  :javac-options ["-source" "1.7" "-target" "1.7"]
  :jvm-opts ^:replace ["-server" "-Xmx1g"]
  :global-vars {*warn-on-reflection* false}
  :min-lein-version "2.5.0")
