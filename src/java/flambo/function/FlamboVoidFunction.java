package flambo.function;

import java.lang.ClassNotFoundException;
import java.io.Serializable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import clojure.lang.AFunction;

import flambo.function.Utils;

import org.apache.spark.api.java.function.VoidFunction;

public class FlamboVoidFunction implements VoidFunction, Serializable {
  
  private AFunction f;
  
  public FlamboVoidFunction() {}
  
  public FlamboVoidFunction(AFunction func) {
    f = func;
  }
  
  public void call(Object v1) throws Exception {
    f.invoke(v1);
  }
  
  private void writeObject(ObjectOutputStream out) throws IOException {
    Utils.writeAFunction(out, f);
  }
  
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    f = Utils.readAFunction(in);
  }
}
