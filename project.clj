(defproject yieldbot/flambo "0.1.0"
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
                 [yieldbot/serializable-fn "0.0.3"]
                 [com.twitter/carbonite "1.3.3"]
                 [com.twitter/chill_2.9.3 "0.3.5"]]
  :plugins [[s3-wagon-private "1.1.2"]]
  :profiles {:provided
             {:dependencies
              [[org.apache.spark/spark-core_2.9.3 "0.8.1-incubating"]]}}
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :aot [flambo.function])
(cemerick.pomegranate.aether/register-wagon-factory!
 "s3p" #(eval '(org.springframework.aws.maven.PrivateS3Wagon.)))
