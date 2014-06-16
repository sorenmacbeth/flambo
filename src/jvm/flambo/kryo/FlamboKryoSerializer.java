package flambo.kryo;

import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.SparkConf;
import com.esotericsoftware.kryo.Kryo;
import carbonite.JavaBridge;
import scala.Tuple2;
import com.twitter.chill.Tuple2Serializer;

public class FlamboKryoSerializer extends KryoSerializer {
  private SparkConf _conf;

  public FlamboKryoSerializer(SparkConf conf) {
    super(conf);
    _conf = conf;
  }

  @Override
  public Kryo newKryo() {
    KryoSerializer ks = new KryoSerializer(_conf);
    Kryo kryo = ks.newKryo();
    try {
      JavaBridge.enhanceRegistry(kryo);
      kryo.register(Tuple2.class, new Tuple2Serializer());
      return kryo;
    } catch (Exception e) {
      throw new RuntimeException("Failed to register Carbonite!");
    }
  }
}
