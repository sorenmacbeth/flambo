(ns flambo.example.tfidf
  (:require [flambo.api :as f]
            [flambo.conf :as conf]
            [flambo.utils :as u]
            [clojure.string :as s])
  (:gen-class))

(def master "local")
(def conf {})
(def env {})

(def stopwords #{"a" "all" "and" "any" "are" "is" "in" "of" "on"
                 "or" "our" "so" "this" "the" "that" "to" "we"})

;; Returns a seq of (doc_id, term) tuples
(f/defsparkfn gen-docid-term-tuples [x]
  (let [[doc-id content] x
        terms (s/split content #" ")]
    (map (fn [term] [doc-id term]) terms)))

;; Filters (doc_id, term) tuples if term is a stopword
(f/defsparkfn filter-stopwords
  [[_ term]]
  (not (contains? stopwords term)))

;; Returns a seq of term-frequency tuples: (term, (doc_id, tf))
(f/defsparkfn term-freq-per-doc
  [[doc-id term-freq-seq]]
  (let [terms-count (reduce (fn [accum [_ term-freq]]
                              (+ accum term-freq))
                            0
                            term-freq-seq)]
    (map (fn [[term term-freq]]
           [term [doc-id (double (/ term-freq terms-count))]])
         term-freq-seq)))

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

          doc-data (-> (f/parallelize sc documents))

          ;; total number of documents in corpus
          num-docs (f/count doc-data)

          ;; stopword filtered RDD of (document_id, term) tuples
          doc-term-seq (-> doc-data
                           (f/flat-map gen-docid-term-tuples)
                           (f/filter filter-stopwords)
                           f/cache)

          ;; tf per document, that is, tf(term, document)
          tf-by-doc (-> doc-term-seq
                        (f/group-by (f/fn [[doc-id term]] [doc-id term]))
                        ;; (raw) number of times a term appears in a document
                        (f/map (f/fn [[[doc-id term] val-seq]] [doc-id [term (count val-seq)]]))
                        f/group-by-key
                        (f/flat-map term-freq-per-doc)
                        f/cache)

          ;; idf of terms, that is, idf(term)
          idf-by-term (-> doc-term-seq
                          f/distinct
                          (f/group-by (f/fn [[_ term]] term))
                          (f/map (f/fn [[term doc-seq]] [term (count doc-seq)])) ;; num of docs with a given term in it
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
