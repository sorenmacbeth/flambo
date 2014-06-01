(ns flambo.kryo
  (:import [org.apache.spark.serializer KryoRegistrator]
           [org.apache.spark SparkEnv]
           [org.apache.spark.serializer SerializerInstance]
           [java.nio ByteBuffer]
           [scala.reflect ClassTag$]))

;; lol scala
(def OBJECT-CLASS-TAG (.apply ClassTag$/MODULE$ java.lang.Object))

(defn class-tag [o]
  (.apply ClassTag$/MODULE$ (class o)))

(defn ^bytes serialize [^Object obj]
  (let [^SerializerInstance ser (.. (SparkEnv/get) serializer newInstance)
        ^ByteBuffer buf (.serialize ser obj (class-tag obj))]
    (.array buf)))

(defn deserialize [^bytes b]
  (let [^ByteBuffer buf (ByteBuffer/wrap b)
        ^SerializerInstance ser (.. (SparkEnv/get) serializer newInstance)]
    (.deserialize ser buf OBJECT-CLASS-TAG)))

(defmacro defregistrator
  [name & register-impl]
  (let [prefix (gensym)
        classname (str *ns* ".registrator." name)]
    `(do
       (gen-class :name ~classname
                  :extends flambo.kryo.BaseFlamboRegistrator
                  :prefix ~prefix)
       (defn ~(symbol (str prefix "register"))
         ~@register-impl)
       (def ~name ~classname))))
