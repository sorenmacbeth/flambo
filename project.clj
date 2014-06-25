(defproject yieldbot/flambo "0.3.0-SNAPSHOT"
  :description "A Clojure DSL for Apache Spark"
  :url "https://github.com/yieldbot/flambo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"releases" {:url "s3p://maven.yieldbot.com/releases/"
                             :username :env :passphrase :env}
                 "snapshots" {:url "s3p://maven.yieldbot.com/snapshots/"
                              :username :env :passphrase :env}}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [yieldbot/serializable-fn "0.0.5"
                  :exclusions [com.twitter/chill-java]]
                 [com.twitter/carbonite "1.3.3"
                  :exclusions [com.twitter/chill-java]]
                 [com.twitter/chill_2.10 "0.3.6"
                  :exclusions [org.scala-lang/scala-library]]]
  :plugins [[s3-wagon-private "1.1.2"]]
  :profiles {:dev
             {:dependencies [[midje "1.6.3"]
                             [criterium "0.4.3"]]
              :plugins [[lein-midje "3.1.3"]
                        [lein-marginalia "0.7.1"]
                        [codox "0.8.9"]]
              ;; so gen-class stuff works in the repl
              :aot [flambo.function]}
             :provided
             {:dependencies
              [[org.apache.spark/spark-core_2.10 "1.0.0"]
               [org.apache.spark/spark-streaming_2.10 "1.0.0"]
               [org.apache.spark/spark-streaming-kafka_2.10 "1.0.0"]
               [org.apache.spark/spark-sql_2.10 "1.0.0"]]}
             :1.5.1
             {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :1.4.0
             {:dependencies [[org.clojure/clojure "1.4.0"]]}}
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :jvm-opts ^:replace []
  :global-vars {*warn-on-reflection* false})
