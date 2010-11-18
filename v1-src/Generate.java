import avro_testing.FooMessage;
import avro_testing.Foo_v1;
import org.apache.avro.util.Utf8;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class Generate {
    
    private static void writeCurrent(String path) throws IOException {
        File f = new File(path);
        OutputStream os = new FileOutputStream(f);
        List<FooMessage> msgs = new ArrayList<FooMessage>();
        for (int i = 0; i < Serialize.COUNT; i++) {
            Foo_v1 foo = new Foo_v1();
            foo.a = new Utf8(Serialize.nextBoolean() ? "1" : "0");
            FooMessage msg = new FooMessage();
            msg.contents = foo;
            Serialize.serializeWithSchema(msg, os);
            msgs.add(msg);
        }
        os.close();
    }
    
    private static void readCurrent(String path) throws IOException {
        File f = new File(path);
        InputStream is = new FileInputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            FooMessage msg = Serialize.deserializeWithSchema(is, new FooMessage());
            assert msg.contents instanceof Foo_v1 : path;
        }
    }
     
    public static void main(String args[]) {
        try {
            Integer version = Integer.parseInt(args[0]);
            Integer nextVersion = version+1;
            
            String currentPath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", version, version)).getPath();
//            writeCurrent(currentPath);
            readCurrent(currentPath);
            
            // this is data written by a future version of the record. it was attempting to write in this old format.
            String futurePath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", version, nextVersion)).getPath();
            if (new File(futurePath).exists()) {
                readCurrent(futurePath);
                System.out.println("v1 can read v1 written by v2");
            }
            // no need to read old.
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
