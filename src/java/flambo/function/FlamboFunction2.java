package flambo.function;

import java.lang.ClassNotFoundException;
import java.io.Serializable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import clojure.lang.AFunction;

import flambo.function.Utils;

import org.apache.spark.api.java.function.Function2;

public class FlamboFunction2 implements Function2, Serializable {
  
  private AFunction f;
  
  public FlamboFunction2() {}
  
  public FlamboFunction2(AFunction func) {
    f = func;
  }
  
  public Object call(Object v1, Object v2) throws Exception {
    return f.invoke(v1, v2);
  }
  
  private void writeObject(ObjectOutputStream out) throws IOException {
    Utils.writeAFunction(out, f);
  }
  
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    f = Utils.readAFunction(in);
  }
}
