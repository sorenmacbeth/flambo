# TF-IDF using flambo

[flambo](https://github.com/yieldbot/flambo) is a Clojure DSL for [Spark](http://spark.apache.org/docs/latest/) created by the data team at [Yieldbot](http://www.yieldbot.com/). It allows you to create and manipulate Spark data structures using idiomatic Clojure. The following tutorial demonstrates typical flambo API usage and facilities by implementing the classic [tf-idf](https://en.wikipedia.org/wiki/Tf-idf) algorithm.

The code for this tutorial is located under the flambo.example.tfidf namespace, under flambo's /test/flambo/example directory. We recommend you download flambo and follow along in your REPL.

### What is tf-idf?

TF-IDF (term frequency-inverse document frequency) is a way to score the importance of terms in a document based on how frequently they appear across a collection of documents (corpus). The tf-idf weight of a term in a document is the product of its `tf` weight:
 
`tf(t, d) = (number of times term t appears in document d) / (total number of terms in document d)`

and its `idf` weight:

`idf(t) = ln(total number of documents in corpus / (1 + number of documents with term t in it))`

## Example Application Walkthrough

First we define our example application's namespace and requires:

```clojure
(ns flambo.example.tfidf
  (:require [flambo.api :as f]
            [flambo.conf :as conf])
  (:gen-class))
```

This will import and alias flambo's `api` and `conf` namespaces, which contain functions to access Spark's API and functions for creating/modifying Spark configuration objects, respectively.

### Initializing Spark

flambo applications require a `SparkContext` object which tells Spark how to access a cluster. The `SparkContext` object requires a `SparkConf` object that encapsulates information about the application. In our [`-main`](https://github.com/yieldbot/flambo/blob/develop/test/flambo/example/tfidf.clj#L35) method we first build a spark configuration, `c`, then pass it to flambo's `spark-context` function which returns the requisite context object, `sc`:

```clojure
(let [c (-> (conf/spark-conf)
            (conf/master master)
            (conf/app-name "tfidf")
            (conf/set "spark.akka.timeout" "300")
            (conf/set conf)
            (conf/set-executor-env env))
      sc (f/spark-context c)])
```

`master` is a special "local" string that tells Spark to run our app in local mode. `master` can be a Spark, Mesos or YARN cluster URL, or any one of the special strings to run in local mode (see [README.md](https://github.com/yieldbot/flambo/blob/develop/README.md#initializing-flambo) for formatting details). 

The `app-name` flambo function is used to set the name of our application. 

As with most distributed computing systems, Spark has a [myriad](http://spark.apache.org/docs/latest/configuration.html) of properties that control most application settings. With flambo you can either `set` these properties directly on a _SparkConf_ object, e.g. `(conf/set "spark.akka.timeout" "300")`, or via a Clojure map, `(conf/set conf)`. Here, we set 
an empty map, `(def conf {})`, for illustration.

Similarly, we set the executor runtime enviroment properties either directly via key/value strings or by passing a Clojure map of key/value strings. `conf/set-executor-env` handles both.

### Computing TF-IDF

Our example uses the following corpus:

```clojure
documents [["doc1" "Four score and seven years ago our fathers brought forth on this continent a new nation"]
           ["doc2" "conceived in Liberty and dedicated to the proposition that all men are created equal"]
           ["doc3" "Now we are engaged in a great civil war testing whether that nation or any nation so"]
           ["doc4" "conceived and so dedicated can long endure We are met on a great battlefield of that war"]]
```

where `doc#` is a unique document id.

We use the corpus and spark context to create a Spark _resilient distributed dataset_ ([RDD](http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds)). There are two ways to create RDDs in flambo: 

* _parallelizing_ an existing Clojure collection, as we've done in our example app:

```clojure
doc-data (f/parallelize sc documents)
```

* [reading](https://github.com/yieldbot/flambo/blob/develop/README.md#external-datasets) a dataset from an external storage system

We are now ready to start applying [_actions_](https://github.com/yieldbot/flambo/blob/develop/README.md#rdd-actions) and [_transformations_](https://github.com/yieldbot/flambo/blob/develop/README.md#rdd-transformations) to our RDD; this is where flambo truly shines (or rather burns bright). It utilizes the powerful abstractions available in Clojure to reason about data. You can use Clojure constructs such as the threading macro `->` to chain sequences of operations and transformations.  

#### Term Frequency

To compute the term freqencies, we need a dictionary of the terms in each document filtered by a set of [_stopwords_](https://github.com/yieldbot/flambo/blob/develop/test/flambo/example/tfidf.clj#L10):

```clojure
doc-term-seq (-> doc-data
                 (f/flat-map gen-docid-term-tuples)
                 (f/filter filter-stopwords)
                 f/cache)
```

We pass the RDD of `[doc-id content]` tuples to flambo's `flat-map` transformation to get a new RDD of `[doc-id term]` tuples, the dictionary for our corpus. 

`flat-map` transforms the source RDD by passing each tuple through a function. It is similar to `map`, but the output is a collection of 0 or more items which is then flattened. Here we use flambo's named function macro `flambo.api/defsparkfn` to define our Clojure function `gen-docid-term-tuples`: 

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


#### Inverse Document Frequency

In order to compute the inverse document frequencies, we need the total number of documents 

```clojure
num-docs (f/count doc-data)
```

and the number of documents that contain each term `t`. The following step groups distinct [doc-id term] tuples by term and maps over the results to count the documents associated with each term. This is combined with the total document count to get an RDD of [term idf] tuples:

```clojure
idf-by-term (-> doc-term-seq
                f/distinct
                (f/group-by (f/fn [[_ term]] term))
                (f/map (f/fn [[term doc-seq]] [term (count doc-seq)])) ;; num of docs containing each term
                (f/map (f/fn [[term df]] [term [(Math/log (/ num-docs (+ 1.0 df)))]]))
                f/cache)
```

#### TF-IDF

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

Finally, [executing](#executing) the example app from the terminal you should see the following output:
 
```bash
$> lein run -m flambo.example.tdidf
$> [["doc1" "score" 0.06301338005090412] ["doc2" "created" 0.09902102579427793] ["doc1" "ago" 0.06301338005090412] ["doc1" "Four" 0.06301338005090412] ["doc1" "forth" 0.06301338005090412] ["doc1" "nation" 0.026152915677434625] ["doc3" "nation" 0.06392934943372908] ["doc1" "brought" 0.06301338005090412] ["doc1" "fathers" 0.06301338005090412] ["doc4" "long" 0.06931471805599453] ["doc1" "seven" 0.06301338005090412] ["doc2" "proposition" 0.09902102579427793] ["doc2" "Liberty" 0.09902102579427793] ["doc4" "battlefield" 0.06931471805599453] ["doc4" "can" 0.06931471805599453] ["doc4" "conceived" 0.028768207245178087] ["doc2" "conceived" 0.041097438921682994] ["doc2" "men" 0.09902102579427793] ["doc3" "civil" 0.07701635339554948] ["doc4" "dedicated" 0.028768207245178087] ["doc2" "dedicated" 0.041097438921682994] ["doc2" "equal" 0.09902102579427793] ["doc3" "Now" 0.07701635339554948] ["doc4" "endure" 0.06931471805599453] ["doc4" "war" 0.028768207245178087] ["doc3" "war" 0.03196467471686454] ["doc3" "testing" 0.07701635339554948] ["doc1" "continent" 0.06301338005090412] ["doc1" "new" 0.06301338005090412] ["doc4" "great" 0.028768207245178087] ["doc3" "great" 0.03196467471686454] ["doc4" "We" 0.06931471805599453] ["doc3" "whether" 0.07701635339554948] ["doc3" "engaged" 0.07701635339554948] ["doc4" "met" 0.06931471805599453] ["doc1" "years" 0.06301338005090412]]
```

You can also save the results to a text file via flambo's `save-as-text-file` function, or an HDFS sequence file via `save-as-sequence-file`, but we'll leave those APIs for you to explore.

### Conclusion

And that's it, we're done! We hope you found this tutorial of the flambo API useful and informative.

flambo is being actively improved, so you can expect more features as Spark continues to grow and we continue to support it. We'd love to hear your feedback on flambo.
