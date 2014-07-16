# flambo

We present a tutorial on how to use [flambo](https://github.com/yieldbot/flambo) to implement the classic [tf-idf](https://en.wikipedia.org/wiki/Tf-idf) algoithm.

Flambo is a Clojure DSL for [Spark](http://spark.apache.org/docs/latest/) created by the data team at [Yieldbot](http://www.yieldbot.com/). It allows you to create and manipulate Spark data structures using idiomatic Clojure.

As we walk-through the tf-idf example application, we'll highlight various flambo API usages and facilities.

The code for the tutorial is located under the `flambo.example.tfidf` namespace, under flambo's [`/test/flambo/example`](https://github.com/yieldbot/flambo/tree/develop/test/flambo/example) directory.

We recommend you download [flambo](https://github.com/yieldbot/flambo) and follow along in your REPL. Let's get started.

### What is tf-idf?

TF-IDF (Term Frequency, Inverse Document Frequency) is a way to score the importance of words (or terms) in a document based on how frequently they appear across multiple doucments (or corpus). The tf-idf weight of a term in a document is the product of its `tf` weight:
 
`tf(t, d) = (number of times term t appears in document d) / (total number of terms in document d)`

and its `idf` weight:

`idf(t) = ln(total number of documents in corpus / number of documents with term t in it)`

## Example Application Walkthrough

First we define our example application's namespace and requires:

```clojure
(ns flambo.example.tfidf
  (:require [flambo.api :as f]
            [flambo.conf :as conf])
  (:gen-class))
```

This will import and alias flambo's `api` and `conf` namespaces. The `(:gen-class)` expression is added so you can execute and inspect the output of the example application from the flambo repo as such:

```bash
$> lein run -m flambo.example.tdidf
```

### Initializing Spark

Flambo applications require a `SparkContext` object, which tells Spark how to access a cluster. `SparkContext` objects in turn require a `SparkConf` object that encapsulates information about the application. In our [`-main`](https://github.com/yieldbot/flambo/blob/develop/test/flambo/example/tfidf.clj#L37) method we first build a spark configuration, `c`, then pass _it_ to flambo's `spark-context` function which returns the requisite context object, `sc`:

```clojure
(let [c (-> (conf/spark-conf)
            (conf/master master)
            (conf/app-name "tfidf")
            (conf/set "spark.akka.timeout" "300")
            (conf/set conf)
            (conf/set-executor-env env))
      sc (f/spark-context c)])
```

`master` is a special "local" string that tells Spark to run our app in local mode. `master` can be a Spark, Mesos or YARN cluster URL, or any one of the special strings to run in local mode (see [README.md](https://github.com/yieldbot/flambo/blob/develop/README.md) for formatting details). 

The `app-name` flambo function is used to set the name of our application. 

As with most distributed computing systems, Spark has a [myriad](http://spark.apache.org/docs/latest/configuration.html) of properties that control most application settings. With flambo you can either `set` these properties directly on a _SparkConf_ object, e.g. `(conf/set "spark.akka.timeout" "300")`, or via a Clojure map, `(conf/set conf)`. Here, we set 
an empty map, `(def conf {})`, for illustration.

Similarly, we set the executor runtime enviroment properties either directly via key/value strings or by passing a Clojure map of key/value strings. `conf/set-executor-env` handles both.

### tf-idf me bro

To keep the output of our example application manageble we take the simplified 'documents' below to define our document space or corpus:

```clojure
documents [["doc1" "Four score and seven years ago our fathers brought forth on this continent a new nation"]
           ["doc2" "conceived in Liberty and dedicated to the proposition that all men are created equal"]
           ["doc3" "Now we are engaged in a great civil war testing whether that nation or any nation so"]
           ["doc4" "conceived and so dedicated can long endure We are met on a great battlefield of that war"]]
```

Note, `doc#` represents the unique document id.

Next, we take our corpus and freshly minted spark-context to create a Spark _resilient distributed dataset_ ([RDD](http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds)). There are two ways to create RDDs in flambo: 

* _parallelizing_ an existing Clojure collection, as we've done in our example app:

```clojure
doc-data (f/parallelize sc documents)
```

* [reading](https://github.com/yieldbot/flambo/blob/develop/README.md#external-datasets) a dataset from an external storage system

We are now ready to start applying [_actions_](https://github.com/yieldbot/flambo/blob/develop/README.md#rdd-actions) and [_transformations_](https://github.com/yieldbot/flambo/blob/develop/README.md#rdd-transformations) to our RDD; this is where flambo truly shines (or rather burns bright). It utilizes the powerful abstractions available in Clojure to reason about data. You can use pure Clojure constructs such as the threading macro `->` to chain sequences of operations and transformations.  

#### Term Frequency

To compute the term freqencies, we need a dictionary of terms in the documents filtered by a set of [_stopwords_](https://github.com/yieldbot/flambo/blob/develop/test/flambo/example/tfidf.clj#L10):

```clojure
doc-term-seq (-> doc-data
                 (f/flat-map gen-docid-term-tuples)
                 (f/filter filter-stopwords)
                 f/cache)
```

We pass the RDD of `[doc-id content]` tuples to flambo's `flat-map` transformation to get a new RDD of `[doc-id term]` tuples, the dictionary for our corpus. 

`flat-map` accomplishes the transformation of the source RDD by passing each tuple element through a function. Here we use flambo's named function macro `flambo.api/defsparkfn` to define our Clojure function `gen-docid-term-tuples`: 

```clojure
(f/defsparkfn gen-docid-term-tuples [x]
  (let [[doc-id content] x
        terms (clojure.string/split content #" ")]
    (map (fn [term] [doc-id term]) terms)))
```

Notice how we use pure Clojure in our Spark function definition to operate on and transform input parameters. Once the Spark function returns, `flat-map` serializes the results back to an RDD for the next action/transformation.

This is flambo's raison d'être. It handles all of the underlying serializations to/from the various Spark Java types, so you only need to define the sequence of operations you would like to perform on your data. That's powerful.

Now although `flat-map` essentially returned the requisite dictionary for our document space, we would like to identify only important terms in documents and filter _stopwords_. `(f/filter filter-stopwords)` does just that, passing our dictionary RDD through the named Spark function `filter-stopwords`:

```clojure
(f/defsparkfn filter-stopwords
  [[_ term]]
  (not (contains? stopwords term)))
```

returning a new RDD of tuples with stopwords dropped.

Having constructed our dictionary we `f/cache` (or _persist_) the dataset in memory for future actions.

Since term-freqency is defined as a function of the document id and term, `tf(document, term)`, we need to group our dictionary RDD by doc-id and term and count the number of times a term appears in a document:

```clojure
tf-by-doc (-> doc-term-seq
              (f/group-by (f/fn [[doc-id term]] [doc-id term]))
              (f/map (f/fn [[[doc-id term] val-seq]] [doc-id [term (count val-seq)]]))
              f/group-by-key
              (f/flat-map term-freq-per-doc)
              f/cache)
```

We use flambo's inline anonymous function macro, `f/fn`, to tell `f/group-by` which fields in the source RDD tuples we would like to group by. In our case, the returned RDD will be comprised of tuples of the form [[doc-id term] seq], where seq is a collection of all tuples equal to the chosen key tuple [doc-id term]. Then, we `map` another flambo inline anonymous
function onto our grouped RDD to generate a new RDD of [doc-id [term count]] tuples. Notice, again how we were able to easily use Clojure's destructuring facilities on our inline function's arguments to name parameters. 

At this point we have an RDD of *raw* term frequencies, but, since we need normalized term frequencies _per_ document we apply a `f/group-by-key` transformation to group tuples by doc-id. This collects all the raw term counts by document id. Then `(f/flat-map term-freq-per-doc)` will flatten those per document sequences and normalize the term frequencies using the named Spark function, `term-freq-per-doc`:

```clojure
(f/defsparkfn term-freq-per-doc
  [[doc-id term-freq-seq]]
  (let [terms-count (reduce (fn [accum [_ term-freq]]
                              (+ accum term-freq))
                            0
                            term-freq-seq)]
    (map (fn [[term term-freq]]
           [term [doc-id (double (/ term-freq terms-count))]])
         term-freq-seq)))
```

Once again, we cache the results for future actions.

And that essentially summarizes how you construct both steps in a flambo application and a full flambo application.

#### Inverse Document Frequency

In order to compute the inverse document frequency for our corpus, we need to determine the total number of documents in the corpus and the number of documents with a term t in it.

```clojure
num-docs (f/count doc-data)
```

Flambo's `count` function above maps to the Spark action _count_ which gives us the number of elements in our original parallelized collection, `doc-data`. We use it together with the following step to get an RDD of [term idf] tuples:

```clojure
idf-by-term (-> doc-term-seq
                f/distinct
                (f/group-by (f/fn [[_ term]] term))
                (f/map (f/fn [[term doc-seq]] [term (count doc-seq)])) ;; num of docs with a given term in it
                (f/map (f/fn [[term df]] [term [(Math/log (/ num-docs (+ 1.0 df)))]]))
                f/cache)
```

Here `f/distinct` is applied to our original stopword filtered RDD, `doc-term-seq`, to insure we don't over count the occurrence of a term in a document.

#### tf-idf

Now that we have both a term-frequency RDD of [term [doc-id tf]] tuples and an inverse-document-frequency RDD of [term idf] tuples, we perform a `join` on the "terms" producing an new RDD of [term [[doc-id tf] [idf]]] tuples. Then, we `map` an inline Spark function to compute the tf-idf weight of each term per document returning our final resulting
RDD of [doc-id term tf-idf] tuples:

```clojure
tfidf-by-term (-> (f/join tf-by-doc idf-by-term)
                  (f/map (f/fn [[term [[doc-id tf] [idf]]]]
                               [doc-id term (* tf idf)]))
                  f/cache)
```

Again, caching the RDD for future actions. Which in our example application is to `collect` all the elements of our tf-idf RDD as a Clojure array and print them to standard out:

```clojure
(-> tfidf-by-term
    f/collect
    clojure.pprint/pprint)
```

Of course, you can also save the results to a text file, via flambo's `save-as-text-file` function, or an HDFS sequence file, via `save-as-sequence-file`, but, we'll leave those APIs for you to explore.

### Conclusion

And that's it, we're done! We hope you found this tutorial of the flambo API useful and informative.

flambo is being actively improved, so you can expect more features as Spark continues to grow and we continue to support it. We'd love to hear your feedback on flambo.
