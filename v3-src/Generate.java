import avro_testing.Bar;
import avro_testing.Foo;
import avro_testing.LikeFoo;
import avro_testing.Message;
import org.apache.avro.util.Utf8;

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
        File file = new File(path);
        OutputStream os = new FileOutputStream(file);
        for (int i = 0; i < Serialize.COUNT; i++) {
            Foo f = new Foo();
            f.a = i % 2 != 0;
            Message fooMessage = new Message();
            fooMessage.version = 3;
            fooMessage.payload = f;
            Serialize.serialize(fooMessage, os, 3);
            
            Bar b = new Bar();
            b.a = i % 2 == 0;
            Message barMessage = new Message();
            barMessage.version = 3;
            barMessage.payload = b;
            Serialize.serialize(barMessage, os, 3);
            
            LikeFoo lf = new LikeFoo();
            lf.a = i % 2 != 0;
            Message lfMessage = new Message();
            lfMessage.version = 3;
            lfMessage.payload = lf;
            Serialize.serialize(lfMessage, os, 3);
        }
        os.close();
    }
    
    private static void readCurrent(String path) throws IOException
    {
        File f = new File(path);
        InputStream is = new FileInputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            Message fooMsg = Serialize.deserialize(is, new Message(), 3);
            assert fooMsg.payload instanceof Foo;
            assert ((Foo)fooMsg.payload).a instanceof Boolean;
            assert (Boolean)((Foo)fooMsg.payload).a == (i % 2 != 0);
            
            Message barMessage = Serialize.deserialize(is, new Message(), 3);
            assert barMessage.payload instanceof Bar;
            assert ((Bar)barMessage.payload).a instanceof Boolean;
            assert (Boolean)((Bar)barMessage.payload).a == (i % 2 == 0);
            
            Message lfMessage = Serialize.deserialize(is, new Message(), 3);
            assert lfMessage.payload instanceof LikeFoo;
            assert ((LikeFoo)lfMessage.payload).a == (Boolean)((Foo)fooMsg.payload).a;
        }
        is.close();
    }
    
    private static void readOld(String path) throws IOException {
        File f = new File(path);
        InputStream is = new FileInputStream(f);
        for (int i = 0; i < Serialize.COUNT; i++) {
            Message fooMessage = Serialize.deserialize(is, new Message(), 2);
            assert fooMessage.version == 2;
            assert fooMessage.payload instanceof Foo;
            fooMessage = translate2to3(fooMessage);
            assert fooMessage.version == 3;
            assert (Boolean)((Foo)fooMessage.payload).a == (i % 2 != 0);
            
            Message barMessage = Serialize.deserialize(is, new Message(), 2);
            assert barMessage.version == 2;
            assert barMessage.payload instanceof Bar;
            barMessage = translate2to3(barMessage);
            assert barMessage.version == 3;
            assert (Boolean)((Bar)barMessage.payload).a == (i % 2 == 0);
        }
    }
    
    private static void writeOld(String readPath, String writePath) throws IOException {
        OutputStream os = new FileOutputStream(writePath);
        InputStream is = new FileInputStream(readPath);
        
        for (int i = 0; i < Serialize.COUNT; i++) {
            // should be interoperable with Foo messages once translated.
            LikeFoo lf = new LikeFoo();
            lf.a = i % 2 != 0;
            Message lfMessage = new Message();
            lfMessage.version = 3;
            lfMessage.payload = lf;
            lfMessage = translate3to2(lfMessage, i);
            assert lfMessage.version == 2;
            Serialize.serialize(lfMessage, os, 2);
            
            Bar b = new Bar();
            b.a = i % 2 == 0;
            Message barMessage = new Message();
            barMessage.version = 3;
            barMessage.payload = b;
            barMessage = translate3to2(barMessage, i);
            assert barMessage.version == 2;
            Serialize.serialize(barMessage, os, 2);
        }
        os.close();
        is.close();
    }
    
    // example translation routines.
    
    private static Message translate3to2(Message msg, int i)
    {
        assert msg.version == 3;
        if (msg.payload instanceof LikeFoo)
        {
            // convert to a real foo. any number of translation schemes would work.
            Foo f = new Foo();
            f.a = ((LikeFoo)msg.payload).a;
            msg.payload = f;
        }
        if (msg.payload instanceof Foo)
            ((Foo)msg.payload).a = i;
        else if (msg.payload instanceof Bar)
            ((Bar)msg.payload).a = i*2;
        else if (msg.payload instanceof LikeFoo) {}
        else
            throw new AssertionError("Unepxected payload");
        msg.version = 2;
        return msg;
    }
    
    private static Message translate2to3(Message msg)
    {
        assert msg.version == 2;
        if (msg.payload instanceof Foo)
        {
            Foo f = (Foo)msg.payload;
            f.a = (Integer)f.a % 2 != 0;
        }
        else if (msg.payload instanceof Bar)
        {
            Bar b = (Bar)msg.payload;
            b.a = (Integer)b.a % 2 == 0;
        }
        else
            throw new AssertionError("Unexpected payload");
        msg.version = 3;
        return msg;
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
                System.out.println("v3 can read v2 written by v2");
            }
            
            // this is data written by a future version of the record. it was attempting to write in this old format.
            String futurePath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", version, nextVersion)).getPath();
            if (new File(futurePath).exists())
                readCurrent(futurePath);
            
            // now write some data in the old format.
            String pastByNewPath = new File(Serialize.GENERATED_DATA, String.format("v%d-by-%d.bin", previousVersion, version)).getPath();
            writeOld(currentPath, pastByNewPath);
            
            System.out.println("v3 tests complete");
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
