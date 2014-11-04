package flambo.function;

import java.lang.ClassNotFoundException;
import java.io.Serializable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import clojure.lang.AFunction;

import flambo.function.Utils;

import org.apache.spark.api.java.function.Function;

public class FlamboFunction implements Function, Serializable {
  
  private AFunction f;
  
  public FlamboFunction() {}
  
  public FlamboFunction(AFunction func) {
    f = func;
  }
  
  public Object call(Object v1) throws Exception {
    return f.invoke(v1);
  }
  
  private void writeObject(ObjectOutputStream out) throws IOException {
    Utils.writeAFunction(out, f);
  }
  
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    f = Utils.readAFunction(in);
  }
}
