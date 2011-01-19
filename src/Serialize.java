
import avro_testing.Message;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Serialize {
    
    public static final File GENERATED_DATA = new File("generated-keep");
    static {
        GENERATED_DATA.mkdirs();
    }
    
    private static final Map<Integer, Schema> SCHEMAS = new HashMap<Integer, Schema>() {{
        put(1, getSchema(1));
        put(2, getSchema(2));
        put(3, getSchema(3));
    }};
    
    private final static DecoderFactory DIRECT_DECODERS = new DecoderFactory().configureDirectDecoder(true);
    
    private static <T extends SpecificRecord> T deserializeWithSchema(InputStream is, T ob, Schema schema) throws IOException {
        BinaryDecoder dec = DIRECT_DECODERS.createBinaryDecoder(is, null);
        SpecificDatumReader<T> reader = new SpecificDatumReader<T>(schema);
        reader.setExpected(ob.getSchema());
        return reader.read(ob, dec);
    }

    private static <T extends SpecificRecord> void serializeWithSchema(T o, OutputStream os, Schema schema) throws IOException {
        BinaryEncoder enc = new BinaryEncoder(os);
        SpecificDatumWriter<T> writer = new SpecificDatumWriter<T>(schema);
        writer.write(o, enc);
        enc.flush();
    }
    
    public static Message deserialize(InputStream is, Message ob, int version) throws IOException {
        assert schemaOk(version) : "Invalid schema " + version;
        Message msg = deserializeWithSchema(is, ob, SCHEMAS.get(version));
        assert msg.version == version;
        return msg;
    }
    
    public static <T extends SpecificRecord> void serialize(Message o, OutputStream os, int version) throws IOException {
        assert o.version == version; 
        assert schemaOk(version) : "Invalid schema " + version;
        serializeWithSchema(o, os, SCHEMAS.get(version));
    }
    
    private static Schema getSchema(int version) {
        try {
            return Schema.parse(new FileInputStream(new File(Serialize.GENERATED_DATA, "v" + version + ".schema")));
        } catch (IOException ex) {
            return null;
        }
    }
    
    public static boolean schemaOk(int version) {
        return SCHEMAS.get(version) != null;
    }
    
    
    // generation utils.
    
    public static final int COUNT = 1;
    private static Random rand = new Random(System.currentTimeMillis());
    public static boolean nextBoolean() {
        return rand.nextBoolean();
    }
}
