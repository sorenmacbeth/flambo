package flambo.kryo;

import com.esotericsoftware.kryo.Kryo;
import carbonite.JavaBridge;
import scala.Tuple2;
import com.twitter.chill.Tuple2Serializer;

public class CarboniteRegistrator {

  public static void registerCarbonite(Kryo kryo) {
    try {
      JavaBridge.enhanceRegistry(kryo);
      kryo.register(Tuple2.class, new Tuple2Serializer());
    } catch (Exception e) {
      throw new RuntimeException("Failed to register Carbonite!");
    }
  }
}
