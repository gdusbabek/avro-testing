
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

public class Serialize {
    
    // codecs
    
    private final static DecoderFactory DIRECT_DECODERS = new DecoderFactory().configureDirectDecoder(true);
    
    public static <T extends SpecificRecord> T deserializeWithSchema(InputStream is, T ob) throws IOException {
        BinaryDecoder dec = DIRECT_DECODERS.createBinaryDecoder(is, null);
        Schema writer = Schema.parse(dec.readString(new Utf8()).toString());
        SpecificDatumReader<T> reader = new SpecificDatumReader<T>(writer);
        reader.setExpected(ob.getSchema());
        return reader.read(ob, dec);
    }

    public static <T extends SpecificRecord> void serializeWithSchema(T o, OutputStream os) throws IOException {
        BinaryEncoder enc = new BinaryEncoder(os);
        enc.writeString(new Utf8(o.getSchema().toString()));
        SpecificDatumWriter<T> writer = new SpecificDatumWriter<T>(o.getSchema());
        writer.write(o, enc);
        enc.flush();
        return;
    }
    
    
    // generation utils.
    
    public static final int COUNT = 1000;
    private static Random rand = new Random(System.currentTimeMillis());
    public static boolean nextBoolean() {
        return rand.nextBoolean();
    }
    
    public static final File GENERATED_DATA = new File("generated-keep");
    static {
        GENERATED_DATA.mkdirs();
    }
}
