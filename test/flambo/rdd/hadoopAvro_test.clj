(ns flambo.rdd.hadoopAvro-test
  (:require [flambo.rdd.hadoopAvro :refer :all]
            [flambo.api :as f]
            [flambo.conf :as conf]
            [midje.sweet :refer :all]))


;; Thanks to Michael G. Noll (https://github.com/miguno)
;; I copied his avro test files from https://github.com/miguno/avro-hadoop-starter

(facts
  "about other stuff"

  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name "hadoop-avro-test"))]
    (f/with-context c conf
                    (fact
                      "load stuff from hadoop using AVRO"
                      (f/collect (load-avro-file c "data/avro/twitter.avro"))
                      => [{:username "miguno", :tweet "Rock: Nerf paper, scissors is fine.", :timestamp 1366150681}
                          {:username "BlizzardCS", :tweet "Works as intended.  Terran is IMBA.", :timestamp 1366154481}
                          {:username "DarkTemplar", :tweet "From the shadows I come!", :timestamp 1366154681}
                          {:username "VoidRay", :tweet "Prismatic core online!", :timestamp 1366160000}
                          {:username "VoidRay", :tweet "Fire at will, commander.", :timestamp 1366160010}
                          {:username "DarkTemplar", :tweet "I am the blade of Shakuras!", :timestamp 1366174681}
                          {:username "Immortal", :tweet "I return to serve!", :timestamp 1366175681}
                          {:username "Immortal", :tweet "En Taro Adun!", :timestamp 1366176283}
                          {:username "VoidRay", :tweet "There is no greater void than the one between your ears.", :timestamp 1366176300}
                          {:username "DarkTemplar", :tweet "I strike from the shadows!", :timestamp 1366184681}]
                      ))))