import avro_testing.Foo;
import org.apache.avro.util.Utf8;

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
        for (int i = 0; i < Serialize.COUNT; i++) {
            Foo foo = new Foo(); // v2
            foo.a = Serialize.nextBoolean() ? 1 : 0;
            Serialize.serialize(foo, os, 2);
        }
        os.close();
    }
    
    private static void readCurrent(String path) throws IOException
    {
        File f = new File(path);
        InputStream is = new FileInputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            Foo msg = Serialize.deserialize(is, new Foo(), 2);
            assert msg.a instanceof Integer : path;
            convert2to1(msg);
        }
    }
    
    private static void readOld(String path) throws IOException {
        File f = new File(path);
        InputStream is = new FileInputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            Foo msg = Serialize.deserialize(is, new Foo(), 1);
            assert msg.a instanceof Utf8 : path;
            convert1to2(msg);
        }
    }
    
    private static void writeOld(String readPath, String writePath) throws IOException {
        OutputStream os = new FileOutputStream(writePath);
        InputStream is = new FileInputStream(readPath);
        
        for (int i = 0; i < Serialize.COUNT; i++) {
            Foo msg = Serialize.deserialize(is, new Foo(), 2);
            Foo v1 = convert2to1(msg);
            Serialize.serialize(v1, os, 1);
        }
        os.close();
        is.close();
    }
    
    // example translation routines.
    
    private static Foo convert1to2(Foo v1) {
        v1.a = Integer.parseInt(v1.a.toString());
        return v1;
    }
    
    private static Foo convert2to1(Foo v2) {
        v2.a = Integer.toString((Integer)v2.a);
        return v2;
    }
     
    public static void main(String args[]) {
        try {
            Integer version = Integer.parseInt(args[0]);
            Integer nextVersion = version+1;
            Integer previousVersion = version-1;
            
            // save the current schema.
            OutputStream os = new FileOutputStream(new File(Serialize.GENERATED_DATA, "v" + version + ".schema"));
            os.write(new Utf8(Foo.SCHEMA$.toString()).getBytes());
            os.close();
            
            String currentPath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", version, version)).getPath();
            writeCurrent(currentPath);
            readCurrent(currentPath);
            
            // read past
            String pastPath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", previousVersion, previousVersion)).getPath();
            if (new File(pastPath).exists()) {
                readOld(pastPath);
                System.out.println("v2 can read v1 written by v1");
            }
            
            // this is data written by a future version of the record. it was attempting to write in this old format.
            String futurePath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", version, nextVersion)).getPath();
            if (new File(futurePath).exists()) {
                readCurrent(futurePath);
                System.out.println("v2 can read v2 writen by v3");
            }
            
            // now write some data in the old format.
            String pastByNewPath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", previousVersion, version)).getPath();
            writeOld(currentPath, pastByNewPath);
            
            System.out.println("v2 tests complete");
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
