package flambo.kryo;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import carbonite.JavaBridge;
import scala.Tuple2;
import com.twitter.chill.Tuple2Serializer;

public class CarboniteRegistrator implements KryoRegistrator {

  @Override
  public void registerClasses(Kryo kryo) {
    try {
      JavaBridge.enhanceRegistry(kryo);
      kryo.register(Tuple2.class, new Tuple2Serializer());
    } catch (Exception e) {
      throw new RuntimeException("Failed to register Carbonite!");
    }
  }
}
