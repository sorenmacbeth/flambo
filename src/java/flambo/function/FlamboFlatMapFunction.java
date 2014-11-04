package flambo.function;

import java.lang.ClassNotFoundException;
import java.io.Serializable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import clojure.lang.AFunction;

import flambo.function.Utils;

import org.apache.spark.api.java.function.FlatMapFunction;

public class FlamboFlatMapFunction implements FlatMapFunction, Serializable {
  
  private AFunction f;
  
  public FlamboFlatMapFunction() {}
  
  public FlamboFlatMapFunction(AFunction func) {
    f = func;
  }
  
  public Iterable<Object> call(Object v1) throws Exception {
    return (Iterable<Object>) f.invoke(v1);
  }
  
  private void writeObject(ObjectOutputStream out) throws IOException {
    Utils.writeAFunction(out, f);
  }
  
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    f = Utils.readAFunction(in);
  }
}
