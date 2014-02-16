(defproject yieldbot/flambo "0.2.0-SNAPSHOT"
  :description "A Clojure DSL for Apache Spark"
  :url "https://github.com/yieldbot/flambo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"releases" {:url "s3p://maven.yieldbot.com/releases/"
                             :username :env :passphrase :env}
                 "snapshots" {:url "s3p://maven.yieldbot.com/snapshots/"
                              :username :env :passphrase :env}}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [yieldbot/serializable-fn "0.0.4"]
                 [com.twitter/carbonite "1.3.3"]
                 [com.twitter/chill_2.10 "0.3.5"]]
  :plugins [[s3-wagon-private "1.1.2"]]
  :profiles {:provided
             {:dependencies
              [[org.apache.spark/spark-core_2.10 "0.9.0-incubating"]]}
             :1.6
             {:dependencies [[org.clojure/clojure "1.6.0-beta1"]]}}
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :global-vars {*warn-on-reflection* true}
  :aot [flambo.function]
  )
(cemerick.pomegranate.aether/register-wagon-factory!
 "s3p" #(eval '(org.springframework.aws.maven.PrivateS3Wagon.)))
