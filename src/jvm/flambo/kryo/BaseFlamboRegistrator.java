package flambo.kryo;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import carbonite.JavaBridge;
import scala.Tuple2;
import com.twitter.chill.Tuple2Serializer;

public class FlamboRegistrator extends Registrator {

  protected void register(Kryo kryo) {
  }

  @Override
  public final void registerClasses(Kryo kryo) {
    try {
      JavaBridge.enhanceRegistry(kryo);
      kryo.register(Tuple2.class, new Tuple2Serializer());
      register(kryo);
    } catch (Exception e) {
      throw new RuntimeException("Failed to register kryo!");
    }
  }
}
