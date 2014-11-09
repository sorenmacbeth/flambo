package flambo.hadoop;

import java.io.IOException;

import clojure.lang.Indexed;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroRecordReaderBase;

/**
 * Created by cbetz on 16.09.14.
 * This is copied over from
 */


public class ClojureAvroRecordReader<K, V>
        extends AvroRecordReaderBase<K, V, Indexed> {
    private K key;

    public ClojureAvroRecordReader(Schema ks, Schema vs) {
        super(null);
        key = null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean nextp = super.nextKeyValue();
        if (nextp) {
            Object item = getCurrentRecord();
            // System.out.println(item);
            key = (K) item;
        } else {
            key = null;
        }
        return nextp;
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public V getCurrentValue()
            throws IOException, InterruptedException {
        return null;
    }
}