import avro_testing.Foo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Generate
{
    private static void writeCurrent(String path) throws IOException
    {
        File f = new File(path);
        OutputStream os = new FileOutputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            Foo foo = new Foo();
            foo.a = Serialize.nextBoolean();
            Serialize.serializeWithSchema(foo, os);
        }
        os.close();
    }
    
    private static void readCurrent(String path) throws IOException
    {
        File f = new File(path);
        InputStream is = new FileInputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            Foo msg = Serialize.deserializeWithSchema(is, new Foo());
            assert msg.a instanceof Boolean : path;
            convert3to2(msg);
        }
    }
    
    private static void readOld(String path) throws IOException {
        File f = new File(path);
        InputStream is = new FileInputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            Foo msg = Serialize.deserializeWithSchema(is, new Foo());
            assert msg.a instanceof Integer : path;
            convert2to3(msg);
        }
    }
    
    private static void writeOld(String readPath, String writePath) throws IOException {
        OutputStream os = new FileOutputStream(writePath);
        InputStream is = new FileInputStream(readPath);
        
        for (int i = 0; i < Serialize.COUNT; i++) {
            Foo msg = Serialize.deserializeWithSchema(is, new Foo());
            Foo v2 = convert3to2(msg);
            Serialize.serializeWithSchema(v2, os);
        }
        os.close();
        is.close();
    }
    
    // example translation routines.
    
    private static Foo convert2to3(Foo v2) {
        v2.a = (Integer)v2.a > 0;
        return v2;
    }
    
    private static Foo convert3to2(Foo v3) {
        v3.a = (Boolean)v3.a ? 1 : 0;
        return v3;
    }
     
    public static void main(String args[]) {
        try {
            Integer version = Integer.parseInt(args[0]);
            Integer nextVersion = version+1;
            Integer previousVersion = version-1;
            
            String currentPath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", version, version)).getPath();
            writeCurrent(currentPath);
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
            writeOld(currentPath, pastByNewPath);
            
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
