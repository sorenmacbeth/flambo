# TF-IDF using flambo

[flambo](https://github.com/yieldbot/flambo) is a Clojure DSL for [Spark](http://spark.apache.org/docs/latest/) created by the data team at [Yieldbot](http://www.yieldbot.com/). It allows you to create and manipulate Spark data structures using idiomatic Clojure. This tutorial demonstrates typical flambo API usage and facilities by implementing the classic [tf-idf](https://en.wikipedia.org/wiki/Tf-idf) algorithm. It is based on the example file [tfidf.clj](https://github.com/yieldbot/flambo/blob/develop/test/flambo/example/tfidf.clj) in the flambo source code.

### What is tf-idf?

TF-IDF (term frequency-inverse document frequency) is a way to score the importance of terms in a document based on how frequently they appear across a collection of documents (a corpus). The tf-idf weight of a term in a document is the product of its `tf` weight:
 
`tf(t, d) = (number of times term t appears in document d) / (total number of terms in document d)`

and its `idf` weight:

`idf(t) = ln(total number of documents in corpus / (1 + number of documents with term t))`

## Example Application Walkthrough

To start with we will need to configure a REPL environment. The easiest way to do this is with [Leiningen](http://leiningen.org/). Once you have installed Leiningen, you can create a new project like so:

```clojure
lein new flambo-tutorial
```

This will create a new Clojure project skelton to work with. This tutorial uses the REPL, but it is convienent to use a project.clj file to tell the REPL what dependencies are required. To do that, modify the project.clj file that Leiningen created to include the flambo dependency: `[yieldbot/flambo "0.8.0"]` (check to ensure you have the correct version of flambo, it may have changed since this was written). Since we are working from the REPL, it is best to set up a dev profile. Mine looks like this:

```clojure
  :profiles {:dev
             ;; so gen-class stuff works in the repl
             {:aot [flambo.function]}
             :provided
             {:dependencies
              [[org.apache.spark/spark-core_2.11 "2.2.0"]
               [org.apache.spark/spark-streaming_2.11 "2.2.0"]
               [org.apache.spark/spark-streaming-kafka-0-8_2.11 "2.2.0"]
               [org.apache.spark/spark-sql_2.11 "2.2.0"]
               [org.apache.spark/spark-hive_2.11 "2.2.0"]]}
             :uberjar
             {:aot :all}}
```

There are more dependencies in this profile than you need for this tutorial, but leave them so that you will be able to complete other tutorials without modifying your project file. From this point you should be able to start a REPL with `lein repl` from the project top-level directory. If things are not working, check the [flambo project.clj](https://github.com/yieldbot/flambo/blob/develop/project.clj) for clues.

### Preliminaries

First, let's start the REPL and load the namespaces we'll need to implement our app. There are three:

```clojure
lein repl
user=> (require '[flambo.api :as f])
user=> (require '[flambo.conf :as conf])
user=> (require '[flambo.tuple :as ft])
```

The flambo `api` and `conf` namespaces contain functions to access Spark's API and to create and modify Spark configuration objects, respectively. The `tuple` namespace contains functions that work with key-value pairs.

### Initializing Spark

flambo applications require a `SparkContext` object which tells Spark how to access a cluster. The `SparkContext` object requires a `SparkConf` object that encapsulates information about the application. With flambo you can either `set` these properties directly on a _SparkConf_ object, e.g., `(conf/set "spark.akka.timeout" "300")`, or via a Clojure map, `(conf/set conf)`. As with most distributed computing systems, Spark has a [myriad of properties](http://spark.apache.org/docs/latest/configuration.html) that control most application settings, but for this example we will just use an empty map and use the `set` method.

```clojure
user=> (def conf {})
```

Similarly, we set the executor runtime enviroment properties either directly via key/value strings or by passing a Clojure map of key/value strings. `conf/set-executor-env` handles both. Again our environment will be empty for this tutorial.

```clojure
user=> (def env {})
```

`master` is a parameter that tells Spark to run our app. `master` can be a Spark, Mesos or YARN cluster URL, or any one of the special strings to run in local mode (see [README.md](https://github.com/yieldbot/flambo/blob/develop/README.md#initializing-flambo) for formatting details). The setting below tells Spark to run locally using all available cores.

```clojure
user=> (def master "local[*]")
```

With the housekeeping out of the way, we can define a spark configuration, `c`, then pass it to the flambo `spark-context` function which returns the requisite context object, `sc`:

``` clojure
user=> (def c (-> (conf/spark-conf)
                  (conf/master master)
                  (conf/app-name "tfidf")
                  (conf/set conf)
                  (conf/set-executor-env env)))
user=> (def sc (f/spark-context c))
```

The `app-name` flambo function is used to set the name of our application. 

### Computing TF-IDF

Our example will use the following corpus:

```clojure
user=> (def documents [(ft/tuple "doc1" "Four score and seven years ago our fathers brought forth on this continent a new nation")
                (ft/tuple "doc2" "conceived in Liberty and dedicated to the proposition that all men are created equal")
                (ft/tuple "doc3" "Now we are engaged in a great civil war testing whether that nation or any nation so")
                (ft/tuple "doc4" "conceived and so dedicated can long endure We are met on a great battlefield of that war")])
```

where `doc#` is a unique document id. We use the corpus and spark context to create a Spark [_resilient distributed dataset_](http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds) (RDD). There are two ways to create RDDs in flambo: 

* _parallelizing_ an existing Clojure collection, as we'll do now:

```clojure
user=> (def doc-data (f/parallelize-pairs sc documents))
```
* [reading external data](https://github.com/yieldbot/flambo/blob/develop/README.md#external-datasets), such as from files, HDFS or S3

We are now ready to start applying [_actions_](https://github.com/yieldbot/flambo/blob/develop/README.md#rdd-actions) and [_transformations_](https://github.com/yieldbot/flambo/blob/develop/README.md#rdd-transformations) to our RDD; this is where flambo truly shines (or rather burns bright). It utilizes the powerful abstractions available in Clojure to reason about data. You can use Clojure constructs such as the threading macro `->` to chain sequences of operations and transformations.

#### Term Frequency

To compute the term freqencies, we need a dictionary of the terms in each document filtered by a set of stopwords. Let's define the stopwords:

```clojure
user=> (def stopwords #{"a" "all" "and" "any" "are" "is" "in" "of" "on" "or" "our" "so" "this" "the" "that" "to" "we"})
```

We would like a filtered dictionary RDD to work with. To get this, we'll write a function and pass the RDD, `doc-data`, of `[doc-id content]` tuples to the flambo `flat-map-to-pair` transformation to get a new stopword filtered RDD of `[doc-id term term-frequency doc-terms-count]` tuples. First we define a Clojure helper function `gen-docid-term-tuples` that will filter stop words and return the tuples we want:

```clojure
user=> (def gen-docid-term-tuples (f/iterator-fn gen-docid-term-tuples [doc-id content]
  (let [terms (filter #(not (contains? stopwords %))
                      (clojure.string/split content #" "))
        doc-terms-count (count terms)
        term-frequencies (frequencies terms)]
    (map (fn [term] (ft/tuple doc-id [term (term-frequencies term) doc-terms-count]))
         (distinct terms))))
```

Notice how we use pure Clojure in our Spark function definition to operate on and transform input parameters. We are able to filter stopwords, determine the number of terms per document and the term-frequencies for each document, all from within Clojure. This is the raison d'être for flambo. It handles all of the underlying serializations to and from the various Spark Java types, so you only need to define the sequence of operations you would like to perform on your data. That's powerful.

Now to create the dictionary, `doc-term-seq` for our corpus we transform the source RDD using our helper function. `flat-map-to-pair` returns a new RDD by first applying a function to all elements of this RDD, and then flattening the results. It is similar to `map`, but the output is a collection of 0 or more key-value pairs which is then flattened.

```clojure
user=> (def doc-term-seq (-> doc-data
                             (f/flat-map-to-pair (ft/key-val-fn gen-docid-term-tuples))
                             f/cache))
```

Once the Spark function returns, `flat-map-to-pair` serializes the results back to an RDD for the next action and transformation. Having constructed our dictionary we `f/cache` (or _persist_) the dataset in memory for future actions.

Recall term-freqency is defined as a function of the document id and term, `tf(document, term)`. At this point we have an RDD of *raw* term frequencies, but we need normalized term frequencies. We use the flambo inline anonymous function macro, `f/fn`, to define an anonymous Clojure function to normalize the frequencies and `map` our `doc-term-seq` RDD of `[doc-id term term-freq doc-terms-count]` tuples to an RDD of key/value, `[term [doc-id tf]]`, tuples. This new tuple format of the term-frequency RDD will be later used to `join` the inverse-document-frequency RDD and compute the final tfidf weights.

```clojure
user=> (def tf-by-doc (-> doc-term-seq
                     (f/map-to-pair (ft/key-val-fn (f/fn [doc-id [term term-freq doc-terms-count]] ; our lambda function
                                                   (ft/tuple term [doc-id (double (/ term-freq doc-terms-count))]))))
                     f/cache))
```

Notice, again how we were easily able to use Clojure's destructuring facilities on the arguments of our inline function to name parameters. As before, we cache the results for future actions.

#### Inverse Document Frequency

In order to compute the inverse document frequencies, we need the total number of documents: 

```clojure
user=> (def num-docs (f/count doc-data))
```

and the number of documents that contain each term.  First, define a helper function to do the calculation. We will use this in `map` in the next step:

```clojure
user=> (defn calc-idf [doc-count]
  (f/fn [term tuple-seq]
    (let [df (count tuple-seq)]
      (ft/tuple term (Math/log (/ doc-count (+ 1.0 df)))))))
```

The following step maps the function we just defined over the distinct `[doc-id term term-freq doc-terms-count]` tuples to count the documents associated with each term. This is combined with the total document count to get an RDD of `[term idf]` tuples.

```clojure
user=> (def idf-by-term (-> doc-term-seq
                          (f/group-by (ft/key-val-fn (f/fn [_ [term _ _]] term)))
                          (f/map-to-pair (ft/key-val-fn (calc-idf num-docs)))))
```

#### TF-IDF

Now that we have both a term-frequency RDD of `[term [doc-id tf]]` tuples and an inverse-document-frequency RDD of `[term idf]` tuples, we perform the aforementioned `join` on the "terms" producing a new RDD of `[term [[doc-id tf] idf]]` tuples. Then, we `map` an inline Spark function to compute the tf-idf weight of each term per document returning our final RDD of `[doc-id term tf-idf]` tuples:

```clojure
user=> (def tfidf-by-term (-> (f/join tf-by-doc idf-by-term)
                            (f/map (ft/key-val-val-fn (f/fn [term [doc-id tf] idf]
                                                        [doc-id term (* tf idf)])))
                            f/cache))
```

Finally, to see the output of our example application we `collect` all the elements of our tf-idf RDD as a Clojure array, sort them by tf-idf weight, and for illustration print the top 10 to standard out:

```clojure
user=> (->> tfidf-by-term
            f/collect
            ((partial sort-by last >))
            (take 10)
            clojure.pprint/pprint)
(["doc2" "created" 0.09902102579427793]
 ["doc2" "men" 0.09902102579427793]
 ["doc2" "Liberty" 0.09902102579427793]
 ["doc2" "proposition" 0.09902102579427793]
 ["doc2" "equal" 0.09902102579427793]
 ["doc3" "civil" 0.07701635339554948]
 ["doc3" "Now" 0.07701635339554948]
 ["doc3" "testing" 0.07701635339554948]
 ["doc3" "engaged" 0.07701635339554948]
 ["doc3" "whether" 0.07701635339554948])
user=> 
```

You can also save the results to a text file via the flambo `save-as-text-file` function, or an HDFS sequence file via `save-as-sequence-file`, but we'll leave those APIs for you to explore.

### Conclusion

And that's it, we're done! We hope you found this tutorial of the flambo API useful and informative.

flambo is being actively improved, so you can expect more features as Spark continues to grow and we continue to support it. We'd love to hear your feedback on flambo.
