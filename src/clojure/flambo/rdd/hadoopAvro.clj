(ns flambo.rdd.hadoopAvro
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            )
  (:import  ;; TODO: Clean imports
           [org.apache.hadoop.io NullWritable]
           [abracad.avro ClojureData]
           [flambo.hadoop ClojureAvroInputFormat]
           [scala Tuple2]
           [org.apache.spark.api.java JavaSparkContext JavaPairRDD]
           [org.apache.avro.hadoop.io AvroSerialization]
           [org.apache.avro.mapreduce AvroKeyOutputFormat AvroJob]
           [org.apache.hadoop.mapreduce Job]))

(f/defsparkfn key-only [^Tuple2 item]
              (._1 item))

(defn load-avro-file
  "This get's me a vector of maps from the avro file."
  [^JavaSparkContext sc path]
  (let [conf (.hadoopConfiguration sc)]
    (AvroSerialization/setDataModelClass conf ClojureData)
  (f/map (.newAPIHadoopFile sc
                            path
                            ClojureAvroInputFormat
                            Object
                            NullWritable
                            conf)
         key-only)))



(defn save-avro-file
  [^JavaSparkContext sc ^JavaPairRDD rdd schema path]
  (let [conf (.hadoopConfiguration sc)
        job (Job. conf)]
    (AvroSerialization/setDataModelClass conf ClojureData)
    (AvroJob/setOutputKeySchema job schema)
    #_(-> rdd
        (f/map key-only)                                    ;; TODO not quite right!!

        )
    (.saveAsNewAPIHadoopFile rdd
                             path
                             Object
                             NullWritable
                             AvroKeyOutputFormat
                             (.getConfiguration job))
    ))




#_(def rdd (flambo.rdd.hadoopAvro/load-avro-file scontext "hdfs://hdfs-master:8020/data/part-m-00000.avro"))
#_(def prdd (flambo.api/map-to-pair rdd (flambo.api/fn [item] [(org.apache.avro.mapred.AvroKey. item) nil])))
#_(flambo.rdd.hadoopAvro/save-avro-file
  scontext
  prdd
  utils.avro-schemas/my-schema
  "hdfs://hdfs-master:8020/data/tmp/test")

