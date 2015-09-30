package flambo.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.spark.serializer.KryoRegistrator;
import carbonite.JavaBridge;
import scala.Tuple2;
import scala.Tuple3;
import com.twitter.chill.Tuple2Serializer;
import com.twitter.chill.Tuple3Serializer;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.Accumulator;

public class BaseFlamboRegistrator implements KryoRegistrator {

  protected void register(Kryo kryo) {
  }

  @Override
  public final void registerClasses(Kryo kryo) {
    try {
      JavaBridge.enhanceRegistry(kryo);
      kryo.register(Tuple2.class, new Tuple2Serializer());
      kryo.register(Tuple3.class, new Tuple3Serializer());
      kryo.register(Broadcast.class, new JavaSerializer());
      kryo.register(Accumulator.class, new JavaSerializer());

      register(kryo);


    } catch (Exception e) {
      throw new RuntimeException("Failed to register kryo!");
    }
  }
}
