package flambo.function;

import flambo.Utils;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import clojure.lang.RT;
import clojure.lang.IFn;
import scala.Tuple2;

import java.util.List;
import java.util.ArrayList;

public class ClojurePairFlatMapFunction extends PairFlatMapFunction<Object, Object, Object> {
  List<String> _fnSpec;
  List<Object> _params;
  PairFlatMapFunction _fn;
  boolean _booted = false;

  public ClojurePairFlatMapFunction(List fnSpec, List<Object> params) {
    _fnSpec = fnSpec;
    _params = params;
  }

  private void bootClojure() {
    if(!_booted) {
      try {
        IFn hof = Utils.loadClojureFn(_fnSpec.get(0), _fnSpec.get(1));
        _fn = (PairFlatMapFunction) hof.applyTo(RT.seq(_params));
        _booted = true;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public Iterable<Tuple2<Object, Object>> call(Object o) throws Exception {
    bootClojure();
    return (Iterable) _fn.call(o);
  }
}
