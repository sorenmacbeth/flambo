package flambo.function;

import flambo.Utils;
import org.apache.spark.api.java.function.PairFunction;
import clojure.lang.RT;
import clojure.lang.IFn;
import scala.Tuple2;

import java.util.List;
import java.util.ArrayList;

public class ClojurePairFunction extends PairFunction<Object, Object, Object> {
  List<String> _fnSpec;
  List<Object> _params;
  PairFunction _fn;
  boolean _booted = false;

  public ClojurePairFunction(List fnSpec, List<Object> params) {
    _fnSpec = fnSpec;
    _params = params;
  }

  private void bootClojure() {
    if(!_booted) {
      try {
        IFn hof = Utils.loadClojureFn(_fnSpec.get(0), _fnSpec.get(1));
        _fn = (PairFunction) hof.applyTo(RT.seq(_params));
        _booted = true;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public Tuple2<Object, Object> call(Object o) throws Exception {
    bootClojure();
    return (Tuple2) _fn.call(o);
  }
}
