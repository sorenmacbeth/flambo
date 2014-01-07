package flambo;

import clojure.lang.IFn;
import clojure.lang.RT;

public class Utils {
  public static IFn loadClojureFn(String namespace, String name) {
    try {
      clojure.lang.Compiler.eval(RT.readString("(require '" + namespace + ")"));
    } catch (Exception e) {
      //if playing from the repl and defining functions, file won't exist
    }
    return (IFn) RT.var(namespace, name).deref();
  }
}
