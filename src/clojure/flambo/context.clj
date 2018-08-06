(ns flambo.context
  (:import [org.apache.spark SparkContext])
  (:require [clojure.tools.logging :as log]
            [flambo.api :as f]
            [flambo.conf :as conf]))

(def ^:dynamic *master*
  "Used when creating Spark contexts in [[create-context]].  This is can be a
  URL or `yarn` for a yarn cluster.  By default this is not necessary to set as
  long as the `spark-submit` job is given the `--deploy-mode` option."
  nil)

(def ^:dynamic *app-name*
  "The application name given to [[create-context]]."
  "flambo")

(defonce ^:private context-inst (atom nil))

(defn databricks-cluster?
  "Return whether or not we're running in a databricks cluster."
  []
  (contains? (System/getProperties) "databricks.serviceName"))

(defn- create-context
  "Create a spark context using URL [*master*].  By default, this creates a
  yarn cluster context."
  []
  (log/infof "creating spark context")
  (if (databricks-cluster?)
    (-> (SparkContext/getOrCreate)
        f/spark-context)
    (-> (conf/spark-conf)
        (conf/app-name *app-name*)
        ((if *master*
           #(conf/master % *master*)
           identity))
        f/spark-context)))

(defn context
  "Return the (single) JVM Spark context.  [*master*] is the URL (defaults to a
  yarn cluster) and used only on the first use of this function.

  See [[close-context]]."
  []
  (swap! context-inst #(or % (create-context))))

(defn close-context
  "Stop and close and cleanup the Spark Context.

  See [[context]]."
  []
  (let [ctx @context-inst]
    (and ctx (.stop ctx)))
  (reset! context-inst nil))
