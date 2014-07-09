(ns flambo.tfidf
  #_(:use [clojure.tools.cli :only [cli]])
  (:require [flambo.api :as f]
            [flambo.conf :as conf]
            [flambo.utils :as u]
            [clojure.string :as s])
  (:gen-class))

#_(def docs-dir "~/repos/flambo/test-docs")
(def master "local")
(def jars (f/jar-of-ns *ns*))
(def conf {})
(def env {})

(def stopwords #{"the" "is"})

(defn -main [& args]
  (try
    (let [#_[{:keys [input output master spark-home jars env spark-executor conf]} _ _]
          #_(cli args
               ["--input" "the input file, a path in local mode, or full hdfs: url for a cluster"]
               ["--output" "the output file.."]
               ["--master" "The master url (local, local[N], spark://..., mesos://..."
                :default (or (System/getenv "MASTER") "local")]
               ["--spark-home" "Path to the Spark home directory."
                :default (System/getenv "SPARK_HOME")]
               ["--jars" (str "A comma separated list of JARs to send to the cluster. These can be paths "
                              "on the local file system or HDFS, HTTP, HTTPS, or FTP URLs.")
                :parse-fn #(s/split % #",") :default this-jar]
               ["--env" "Environment variables to set on worker nodes (e.g. \"k=v,k=v\")"
                :parse-fn #(->> (s/split % #",") (map (fn [s] (s/split s #"="))) (into {}))
                :default {}]
               ["--conf" "Additional key/val pairs to add to SparkConf (e.g. \"k=v,k=v\")"
                :parse-fn #(->> (s/split % #",") (map (fn [s] (s/split s #"="))) (into {}))
                :default {}]
               ["--spark-executor" "path to Spark exectutor"
                :default (System/getenv "SPARK_EXECUTOR_URI")])
          c (-> (conf/spark-conf)
                (conf/master master)
                (conf/app-name "tfidf")
                (conf/jars jars)
                #_(when-> spark-home (conf/spark-home spark-home))
                #_(when-> spark-executor
                        (conf/set "spark.executor.uri" spark-executor))
                (conf/set "spark.executor.memory" "2g")
                (conf/set "spark.kryoserializer.buffer.mb" "10")
                (conf/set "spark.default.parallelism" "288")
                (conf/set "spark.akka.timeout" "300")
                (conf/set conf)
                (conf/set-executor-env env))
          sc (f/spark-context c)
          ;; sample docs and terms
          documents [["doc1" "a b c d"]
                     ["doc2" "a e f g"]
                     ["doc3" "a h i j"]
                     ["doc4" "a b b k l l l l"]]
          doc-data (-> (f/parallelize sc documents))

          ;; total number of documents in corpus
          num-docs (f/count doc-data)

          ;; stopword filtered RDD of (document_id, term) tuples
          doc-term-seq (-> doc-data
                           (f/flat-map
                             (f/fn [x] (let [[doc-id content] x
                                             terms (s/split content #" ")]
                                         (map (fn [term] [doc-id term]) terms))))
                           (f/filter (f/fn [[_ term]] (not (contains? stopwords term))))
                           f/cache)

          ;; (raw) number of times a term appears in a document
          raw-tf-by-doc (-> doc-term-seq
                            (f/group-by (f/fn [[doc-id term]] [doc-id term]))
                            (f/map (f/fn [[[doc-id term] vs]] [doc-id [term (count vs)]]))
                            f/cache)

          ;; total number of terms in a document
          total-terms-in-doc (-> doc-term-seq
                                 f/group-by-key
                                 (f/map (f/fn [[doc-id terms]] [doc-id [(count terms)]]))
                                 f/cache)

          ;; tf per document, that is, tf(term, document)
          tf-by-doc (-> (f/join raw-tf-by-doc total-terms-in-doc)
                        (f/map (f/fn [[doc-id [[term term-feq] [terms-count]]]]
                                     [term [doc-id (double (/ term-feq terms-count))]]))
                        f/cache)

          ;; number of documents with given term in it
          doc-frequencies (-> doc-term-seq
                              (f/group-by (f/fn [[_ term]] term))
                              (f/map (f/fn [[term doc-seq]] [term (count doc-seq)]))
                              f/cache)

          ;; idf of terms, that is, idf(term)
          idf-by-term (-> doc-frequencies
                          (f/map (f/fn [[term df]] [term [(Math/log (/ num-docs (+ 1.0 df)))]]))
                          f/cache)

          ;; tf-idf of terms, that is, tf(term, document) x idf(term)
          tfidf-by-term (-> (f/join tf-by-doc idf-by-term)
                            (f/map (f/fn [[term [[doc-id tf] [idf]]]]
                                         [doc-id term (* tf idf)]))
                            f/cache)
          ]
      (-> tfidf-by-term
          f/collect
          clojure.pprint/pprint))))
