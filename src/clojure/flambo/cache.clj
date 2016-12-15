(ns flambo.cache
  (:import [com.google.common.cache Cache CacheBuilder]
           [java.util.concurrent TimeUnit]))

(defn sized-ttl-memoize
  "Memoize backed by a sized (LRU) ttl guava cache. Keeps metrics on the hit-rate
  of the cache, and the eviction rate."
  [size ttl f & args]
  (let [^CacheBuilder cb (doto (CacheBuilder/newBuilder)
                           (.maximumSize (long size))
                           (.expireAfterWrite (long ttl) TimeUnit/MILLISECONDS)
                           (.recordStats))
        ^Cache c (.build cb)]
    (fn [& args]
      (if-let [v (.getIfPresent c args)]
        v
        (when-let [v (apply f args)]
          (.put c args v)
          v)))))

(defn ttl-memoize
  "Memoize backed by ttl guava cache"
  [ttl f & args]
  (let [^CacheBuilder builder (doto (CacheBuilder/newBuilder)
                                (.expireAfterWrite (long ttl) TimeUnit/MILLISECONDS))
        ^Cache c (.build builder)]
    (fn [& args]
      (if-let [v (.getIfPresent c args)]
        v
        (when-let [v (apply f args)]
          (.put c args v)
          v)))))

(defn lru-memoize
  "Memoize backed by maximum size (LRU) guava cache"
  [size f & args]
  (let [^CacheBuilder builder (doto (CacheBuilder/newBuilder)
                                (.maximumSize (long size)))
        ^Cache c (.build builder)]
    (fn [& args]
      (if-let [v (.getIfPresent c args)]
        v
        (when-let [v (apply f args)]
          (.put c args v)
          v)))))
