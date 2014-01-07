package flambo.function;

import flambo.Utils;
import org.apache.spark.api.java.function.Function2;
import clojure.lang.RT;
import clojure.lang.IFn;

import java.util.List;
import java.util.ArrayList;

public class ClojureFunction2 extends Function2<Object, Object, Object> {
  List<String> _fnSpec;
  List<Object> _params;
  Function2 _fn;
  boolean _booted = false;

  public ClojureFunction2(List fnSpec, List<Object> params) {
    _fnSpec = fnSpec;
    _params = params;
  }

  private void bootClojure() {
    if(!_booted) {
      try {
        IFn hof = Utils.loadClojureFn(_fnSpec.get(0), _fnSpec.get(1));
        _fn = (Function2) hof.applyTo(RT.seq(_params));
        _booted = true;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public Object call(Object o1, Object o2) throws Exception {
    bootClojure();
    return _fn.call(o1, o2);
  }
}
