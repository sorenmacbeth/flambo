package flambo.function;

import java.lang.Iterable;
import java.util.Iterator;
import java.lang.ClassNotFoundException;
import java.io.Serializable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import clojure.lang.AFunction;
import clojure.lang.Indexed;

import flambo.function.Utils;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class FlamboPairFlatMapFunction implements PairFlatMapFunction, Serializable {
  
  private AFunction f;
  
  public FlamboPairFlatMapFunction() {}
  
  public FlamboPairFlatMapFunction(AFunction func) {
    f = func;
  }
  
  public Iterable<Tuple2<Object, Object>> call(Object v1) throws Exception {
    final Iterable<Indexed> values = (Iterable<Indexed>) f.invoke(v1);
    final Iterator<Indexed> iter = values.iterator();
    
    // Transforms every Indexed objects to Tuple2
    return new Iterable<Tuple2<Object,Object>>() {
      public Iterator<Tuple2<Object,Object>> iterator() {
        return new Iterator() {
          public boolean hasNext() {
            return iter.hasNext();
          }
          
          public Tuple2 next() {
            Indexed v = iter.next();
            return new Tuple2(v.nth(0), v.nth(1));
          }
          
          public void remove(){
            iter.remove();
          }
        };
      }
    };
  }
  
  private void writeObject(ObjectOutputStream out) throws IOException {
    Utils.writeAFunction(out, f);
  }
  
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    f = Utils.readAFunction(in);
  }
}
