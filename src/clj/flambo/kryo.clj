(ns flambo.kryo
  (:import [org.apache.spark.serializer KryoRegistrator]
           [org.apache.spark SparkEnv]
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

(defmacro defregistrator
  [name register-impl]
  (let [prefix (gensym)
        classname (str *ns* ".registrator." name)]
    `(do
       (gen-class :name ~classname
                  :implements [org.apache.spark.serializer.KryoRegistrator]
                  :prefix ~prefix)
       (defn ~(symbol (str prefix "registerClasses"))
         ~@register-impl)
       (def ~name ~classname))))
