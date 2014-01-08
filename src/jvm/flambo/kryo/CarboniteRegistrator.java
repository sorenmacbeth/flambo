package flambo.kryo;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import carbonite.JavaBridge;

public class CarboniteRegistrator implements KryoRegistrator {
  @Override
  public void registerClasses(Kryo kryo) {
    try {
      JavaBridge.enhanceRegistry(kryo);
    } catch (Exception e) {
      throw new RuntimeException("Failed to register Carbonite!");
    }
  }
}
