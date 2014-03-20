(ns flambo.kryo
  (:import [org.apache.spark SparkEnv]
           [org.apache.spark.serializer SerializerInstance]
           [java.nio ByteBuffer]))

(defn ^bytes serialize [^Object obj]
  (let [^SerializerInstance ser (.. (SparkEnv/get) serializer newInstance)
        ^ByteBuffer buf (.serialize ser obj)]
    (.array buf)))

(defn deserialize [^bytes b]
  (let [^ByteBuffer buf (ByteBuffer/wrap b)
        ^SerializerInstance ser (.. (SparkEnv/get) serializer newInstance)]
    (.deserialize ser buf)))
