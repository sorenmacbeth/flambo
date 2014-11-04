(ns flambo.function)

(defmacro gen-function
  [clazz wrapper-name]
  `(defn ~wrapper-name [f#]
     (new ~(symbol (str "flambo.function." clazz)) f#)))

(gen-function FlamboFunction function)
(gen-function FlamboFunction2 function2)
(gen-function FlamboFunction3 function3)
(gen-function FlamboVoidFunction void-function)
(gen-function FlamboFlatMapFunction flat-map-function)
(gen-function FlamboFlatMapFunction2 flat-map-function2)
(gen-function FlamboPairFlatMapFunction pair-flat-map-function)
(gen-function FlamboPairFunction pair-function)
