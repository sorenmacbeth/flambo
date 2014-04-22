package flambo.kryo;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import carbonite.JavaBridge;
import scala.Tuple2;
import com.twitter.chill.Tuple2Serializer;

public class FlamboRegistrator implements KryoRegistrator {

  @Override
  public void registerClasses(Kryo kryo) {
    CarboniteRegistrator.registerCarbonite(kryo);
  }
}
