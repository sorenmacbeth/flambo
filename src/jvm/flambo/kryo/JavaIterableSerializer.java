package flambo.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.lang.reflect.*;

public class JavaIterableSerializer<T> extends Serializer<T> {
  Class cls;
  Method underlyingMethod;

  public JavaIterableSerializer() {
    try {
      this.cls = Class.forName("scala.collection.convert.Wrappers$IterableWrapper");
      this.underlyingMethod = cls.getDeclaredMethod("underlying");
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException("class not found!");
    } catch (NoSuchMethodException nsme) {
      throw new RuntimeException("method not found!");
    }
  }

  @Override
  public void write(Kryo kryo, Output output, T obj) {
    try {
      kryo.writeClassAndObject(output, underlyingMethod.invoke(obj));
    } catch (IllegalAccessException iae) {
      throw new RuntimeException("illegal access!");
    } catch (InvocationTargetException ite) {
      throw new RuntimeException("invocation target!");
    }
  }

  @Override
  public T read(Kryo kryo, Input input, Class<T> type) {
    Object underlying = kryo.readClassAndObject(input);
    return (T) scala.collection.JavaConversions.asJavaIterable((scala.collection.Iterable<T>) underlying);
  }
}
