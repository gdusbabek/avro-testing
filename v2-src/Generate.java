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
import java.util.ArrayList;
import java.util.List;

public class Generate
{
    private static void writeCurrent(String path) throws IOException
    {
        File f = new File(path);
        OutputStream os = new FileOutputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            Foo foo = new Foo();
            foo.a = i;
            Message fooMessage = new Message();
            fooMessage.version = 2;
            fooMessage.payload = foo;
            Serialize.serialize(fooMessage, os, 2);
            
            Bar bar = new Bar();
            bar.a = i*2;
            Message barMessage = new Message();
            barMessage.version = 2;
            barMessage.payload = bar;
            Serialize.serialize(barMessage, os, 2);
        }
        os.close();
    }
    
    private static void readCurrent(String path) throws IOException
    {
        File f = new File(path);
        InputStream is = new FileInputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            Message fooMessage = Serialize.deserialize(is, new Message(), 2);
            assert fooMessage.payload instanceof Foo;
            assert (Integer)((Foo)fooMessage.payload).a == i;
            
            Message barMessage = Serialize.deserialize(is, new Message(), 2);
            assert barMessage.payload instanceof Bar;
            assert (Integer)((Bar)barMessage.payload).a == i*2;
        }
    }
    
    private static Message translate2to1(Message v2)
    {
        assert v2.version == 2;
        if (v2.payload instanceof Foo)
        {
            Foo f = (Foo)v2.payload;
            f.a = Integer.toString((Integer)f.a);
        }
        else if (v2.payload instanceof Bar)
        {
            Bar b = (Bar)v2.payload;
            b.a = Integer.toString((Integer)b.a);
        }
        else
            throw new AssertionError("Couldn't translate");
        v2.version = 1;
        return v2;
    }
    
    private static Message translate1to2(Message v1)
    {
        assert v1.version == 1;
        if (v1.payload instanceof Foo)
        {
            Foo f = (Foo)v1.payload;
            f.a = Integer.parseInt(f.a.toString());
        }
        else if (v1.payload instanceof Bar)
        {
            Bar b = (Bar)v1.payload;
            b.a = Integer.parseInt(b.a.toString());
        }
        v1.version = 2;
        return v1;
    }
    
    private static void readOld(String path) throws IOException {
        File f = new File(path);
        InputStream is = new FileInputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            Message fooMessage = Serialize.deserialize(is, new Message(), 1);
            assert fooMessage.payload instanceof Foo;
            assert ((Foo)fooMessage.payload).a instanceof Utf8;
            fooMessage = translate1to2(fooMessage);
            assert (Integer)((Foo)fooMessage.payload).a == i;
            
            Message barMessage = Serialize.deserialize(is, new Message(), 1);
            assert barMessage.payload instanceof Bar;
            assert ((Bar)barMessage.payload).a instanceof Utf8;
            barMessage = translate1to2(barMessage);
            assert (Integer)((Bar)barMessage.payload).a == i*2;
        }
        is.close();
    }
    
    private static void writeOld(String readPath, String writePath) throws IOException {
        OutputStream os = new FileOutputStream(writePath);
        InputStream is = new FileInputStream(readPath);
        
        for (int i = 0; i < Serialize.COUNT; i++) {
            Foo f = new Foo();
            f.a = i;
            Message fooMsg = new Message();
            fooMsg.version = 2;
            fooMsg.payload = f;
            fooMsg = translate2to1(fooMsg);
            Serialize.serialize(fooMsg, os, 1);
            
            Bar b = new Bar();
            b.a = i*2;
            Message barMsg = new Message();
            barMsg.version = 2;
            barMsg.payload = b;
            barMsg = translate2to1(barMsg);
            Serialize.serialize(barMsg, os, 1);
        }
        os.close();
        is.close();
    }
    
    public static void main(String args[]) {
        try {
            Integer version = Integer.parseInt(args[0]);
            Integer nextVersion = version+1;
            Integer previousVersion = version-1;
            
            // save the current schema.
            OutputStream os = new FileOutputStream(new File(Serialize.GENERATED_DATA, "v" + version + ".schema"));
            os.write(new Utf8(Message.SCHEMA$.toString()).getBytes());
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
