package structure;

import java.util.Collections;
import java.util.List;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.backtype.hadoop.pail.PailStructure;

public abstract class ThriftPailStructure<T extends Comparable> implements PailStructure<T> {

    protected abstract T createThriftObject();

    private transient TDeserializer des;

    private TDeserializer getDeserializer() {
        if (des == null) {
            des = new TDeserializer();
        }
        return des;
    }

    @Override
    public T deserialize(byte[] record) {
        T ret = createThriftObject();
        try {
            getDeserializer().deserialize((TBase) ret, record);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    private transient TSerializer ser;

    private TSerializer getSerializer() {
        if (ser == null) {
            ser = new TSerializer();
        }
        return ser;
    }

    @Override
    public byte[] serialize(T obj) {
        try {
            return getSerializer().serialize((TBase) obj);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isValidTarget(String... dirs) {
        return true;
    }

    @Override
    public List<String> getTarget(T object) {
        return Collections.EMPTY_LIST;
    }
}
