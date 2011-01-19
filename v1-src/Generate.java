import avro_testing.Bar;
import avro_testing.Foo;
import avro_testing.Message;
import org.apache.avro.util.Utf8;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Generate {
    
    private static void writeCurrent(String path) throws IOException {
        File f = new File(path);
        OutputStream os = new FileOutputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            // strings, version 1
            Foo foo = new Foo();
            foo.a = new Utf8(Integer.toString(i));
            Message fooMessage = new Message();
            fooMessage.version = 1;
            fooMessage.payload = foo;
            Serialize.serialize(fooMessage, os, 1);
            
            Bar bar = new Bar();
            bar.a = new Utf8(Integer.toString(i*2));
            Message barMessage = new Message();
            barMessage.version = 1;
            barMessage.payload = bar;
            Serialize.serialize(barMessage, os, 1);
        }
        os.close();
    }
    
    private static void readCurrent(String path) throws IOException {
        File f = new File(path);
        InputStream is = new FileInputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            Message fooMessage = Serialize.deserialize(is, new Message(), 1);
            assert fooMessage.payload instanceof Foo;
            assert ((Foo)fooMessage.payload).a.toString().equals(Integer.toString(i)) : i + ", " + fooMessage;
            Message barMessage = Serialize.deserialize(is, new Message(), 1);
            assert barMessage.payload instanceof Bar;
            assert ((Bar)barMessage.payload).a.toString().equals(Integer.toString(i*2));
        }
        is.close();
    }
     
    public static void main(String args[]) {
        try {
            Integer version = Integer.parseInt(args[0]);
            Integer nextVersion = version+1;
            
            // save the current schema.
            OutputStream os = new FileOutputStream(new File(Serialize.GENERATED_DATA, "v" + version + ".schema"));
            os.write(new Utf8(Message.SCHEMA$.toString()).getBytes());
            os.close();
            
            String currentPath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", version, version)).getPath();
            writeCurrent(currentPath);
            readCurrent(currentPath);
            
            // this is data written by a future version of the record. it was attempting to write in this old format.
            String futurePath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", version, nextVersion)).getPath();
            if (new File(futurePath).exists()) {
                readCurrent(futurePath);
                System.out.println("v1 can read v1 written by v2");
            }
            
            // read old
            // no need to read old. this is v1.
            
            System.out.println("v1 tests complete");
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
