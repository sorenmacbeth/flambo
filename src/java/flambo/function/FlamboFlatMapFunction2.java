package flambo.function;

import java.lang.ClassNotFoundException;
import java.io.Serializable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import clojure.lang.AFunction;

import flambo.function.Utils;

import org.apache.spark.api.java.function.FlatMapFunction2;

public class FlamboFlatMapFunction2 implements FlatMapFunction2, Serializable {
  
  private AFunction f;
  
  public FlamboFlatMapFunction2() {}
  
  public FlamboFlatMapFunction2(AFunction func) {
    f = func;
  }
  
  public Iterable<Object> call(Object v1, Object v2) throws Exception {
    return (Iterable<Object>) f.invoke(v1, v2);
  }
  
  private void writeObject(ObjectOutputStream out) throws IOException {
    Utils.writeAFunction(out, f);
  }
  
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    f = Utils.readAFunction(in);
  }
}
