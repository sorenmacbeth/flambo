(ns flambo.example.tfidf
  (:require [flambo.api :as f]
            [flambo.conf :as conf])
  (:gen-class))

(def master "local")
(def conf {})
(def env {})

(def stopwords #{"a" "all" "and" "any" "are" "is" "in" "of" "on"
                 "or" "our" "so" "this" "the" "that" "to" "we"})

;; Returns a stopword filtered seq of
;; [doc-id term term-frequency doc-terms-count] tuples
(f/defsparkfn gen-docid-term-tuples [doc-tuple]
  (let [[doc-id content] doc-tuple
        terms (filter #(not (contains? stopwords %))
                      (clojure.string/split content #" "))
        doc-terms-count (count terms)
        term-frequencies (frequencies terms)]
    (map (fn [term] [doc-id term (term-frequencies term) doc-terms-count])
         (distinct terms))))

(defn calc-idf [doc-count]
  (f/fn [[term tuple-seq]]
    (let [df (count tuple-seq)]
      [term (Math/log (/ doc-count (+ 1.0 df)))])))

(defn -main [& args]
  (try
    (let [c (-> (conf/spark-conf)
                (conf/master master)
                (conf/app-name "tfidf")
                (conf/set "spark.akka.timeout" "300")
                (conf/set conf)
                (conf/set-executor-env env))
          sc (f/spark-context c)

          ;; sample docs and terms
          documents [["doc1" "Four score and seven years ago our fathers brought forth on this continent a new nation"]
                     ["doc2" "conceived in Liberty and dedicated to the proposition that all men are created equal"]
                     ["doc3" "Now we are engaged in a great civil war testing whether that nation or any nation so"]
                     ["doc4" "conceived and so dedicated can long endure We are met on a great battlefield of that war"]]

          doc-data (f/parallelize sc documents)

          ;; stopword filtered RDD of [doc-id term term-freq doc-terms-count] tuples
          doc-term-seq (-> doc-data
                           (f/flat-map gen-docid-term-tuples)
                           f/cache)

          ;; RDD of term-frequency tuples: [term [doc-id tf]]
          ;; where tf is per document, that is, tf(term, document)
          tf-by-doc (-> doc-term-seq
                        (f/map (f/fn [[doc-id term term-freq doc-terms-count]]
                                     [term [doc-id (double (/ term-freq doc-terms-count))]]))
                        f/cache)

          ;; total number of documents in corpus
          num-docs (f/count doc-data)

          ;; idf of terms, that is, idf(term)
          idf-by-term (-> doc-term-seq
                          (f/group-by (f/fn [[_ term _ _]] term))
                          (f/map (calc-idf num-docs))
                          f/cache)

          ;; tf-idf of terms, that is, tf(term, document) x idf(term)
          tfidf-by-term (-> (f/join tf-by-doc idf-by-term)
                            (f/map (f/fn [[term [[doc-id tf] idf]]]
                                         [doc-id term (* tf idf)]))
                            f/cache)
          ]
      (->> tfidf-by-term
           f/collect
           ((partial sort-by last >))
           (take 10)
           clojure.pprint/pprint))
    (catch Exception e
      (println (.printStackTrace e)))))
