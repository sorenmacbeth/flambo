package flambo.kryo;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import carbonite.JavaBridge;
import scala.Tuple2;
import scala.Tuple3;
import com.twitter.chill.Tuple2Serializer;
import com.twitter.chill.Tuple3Serializer;

public class BaseFlamboRegistrator implements KryoRegistrator {

  protected void register(Kryo kryo) {
  }

  @Override
  public final void registerClasses(Kryo kryo) {
    try {
      JavaBridge.enhanceRegistry(kryo);
      kryo.register(Tuple2.class, new Tuple2Serializer());
      kryo.register(Tuple3.class, new Tuple3Serializer());

      register(kryo);


    } catch (Exception e) {
      throw new RuntimeException("Failed to register kryo!");
    }
  }
}
