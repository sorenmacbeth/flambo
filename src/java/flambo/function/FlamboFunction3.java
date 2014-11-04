package flambo.function;

import java.lang.ClassNotFoundException;
import java.io.Serializable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import clojure.lang.AFunction;

import flambo.function.Utils;

import org.apache.spark.api.java.function.Function3;

public class FlamboFunction3 implements Function3, Serializable {
  
  private AFunction f;
  
  public FlamboFunction3() {}
  
  public FlamboFunction3(AFunction func) {
    f = func;
  }
  
  public Object call(Object v1, Object v2, Object v3) throws Exception {
    return f.invoke(v1, v2, v3);
  }
  
  private void writeObject(ObjectOutputStream out) throws IOException {
    Utils.writeAFunction(out, f);
  }
  
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    f = Utils.readAFunction(in);
  }
}
