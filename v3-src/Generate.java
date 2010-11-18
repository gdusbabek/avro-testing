import avro_testing.FooMessage;
import avro_testing.Foo_v2;
import avro_testing.Foo_v3;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class Generate
{
    private static void writeCurrent(String path) throws IOException
    {
        File f = new File(path);
        OutputStream os = new FileOutputStream(f);
        List<FooMessage> msgs = new ArrayList<FooMessage>();
        for (int i = 0; i < Serialize.COUNT; i++) {
            Foo_v3 foo = new Foo_v3();
            foo.c = Serialize.nextBoolean();
            FooMessage msg = new FooMessage();
            msg.contents = foo;
            Serialize.serializeWithSchema(msg, os);
            msgs.add(msg);
        }
        os.close();
    }
    
    private static void readCurrent(String path) throws IOException
    {
        File f = new File(path);
        InputStream is = new FileInputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            FooMessage msg = Serialize.deserializeWithSchema(is, new FooMessage());
            assert msg.contents instanceof Foo_v3 : path;
            convert((Foo_v3)msg.contents);
        }
    }
    
    private static void readOld(String path) throws IOException {
        File f = new File(path);
        InputStream is = new FileInputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            FooMessage msg = Serialize.deserializeWithSchema(is, new FooMessage());
            assert msg.contents instanceof Foo_v2 : path;
            convert((Foo_v2)msg.contents);
        }
    }
    
    private static void writeOld(String readPath, String writePath) throws IOException {
        OutputStream os = new FileOutputStream(writePath);
        InputStream is = new FileInputStream(readPath);
        
        for (int i = 0; i < Serialize.COUNT; i++) {
            FooMessage msg = Serialize.deserializeWithSchema(is, new FooMessage());
            Foo_v2 v2 = convert((Foo_v3)msg.contents);
            FooMessage msgv2 = new FooMessage();
            msgv2.contents = v2;
            Serialize.serializeWithSchema(msgv2, os);
        }
        os.close();
        is.close();
    }
    
    // example translation routines.
    
    private static Foo_v3 convert(Foo_v2 v2) {
        Foo_v3 v3 = new Foo_v3();
        v3.c = v2.b > 0;
        return v3;
    }
    
    private static Foo_v2 convert(Foo_v3 v3) {
        Foo_v2 v2 = new Foo_v2();
        v2.b = v3.c ? 1 : 0;
        return v2;
    }
    
     
    public static void main(String args[]) {
        try {
            Integer version = Integer.parseInt(args[0]);
            Integer nextVersion = version+1;
            Integer previousVersion = version-1;
            
            String currentPath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", version, version)).getPath();
//            writeCurrent(currentPath);
            readCurrent(currentPath);
            
            // read past
            String pastPath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", previousVersion, previousVersion)).getPath();
            if (new File(pastPath).exists()) {
                readOld(pastPath);
                System.out.println("v3 can read v2 written by v2");
            }
            
            // this is data written by a future version of the record. it was attempting to write in this old format.
            String futurePath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", version, nextVersion)).getPath();
            if (new File(futurePath).exists())
                readCurrent(futurePath);
            
            // now write some data in the old format.
            String pastByNewPath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", previousVersion, version)).getPath();
//            writeOld(currentPath, pastByNewPath);
            
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
