import java.nio.file.*;
import org.apache.lucene.store.*;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
public class CaptureSelfTest {
 public static void main(String[] args)throws Exception {
  Path path=Path.of(args[0]); Files.createDirectories(path);
  try(var dir=new NIOFSDirectory(path)) {
   try(var out=dir.createOutput("test.bin",IOContext.DEFAULT)) {
    CodecUtil.writeHeader(out,"capture-test",1);
    for(int i=0;i<100000;i++)out.writeByte((byte)i);
    CodecUtil.writeFooter(out);
   }
   try(var in=dir.openInput("test.bin",IOContext.DEFAULT)){CodecUtil.checksumEntireFile(in);}
   byte[] data=Files.readAllBytes(path.resolve("test.bin"));data[1234]^=1;Files.write(path.resolve("test.bin"),data);
   boolean caught=false;
   try(var in=dir.openInput("test.bin",IOContext.DEFAULT)){CodecUtil.checksumEntireFile(in);}catch(CorruptIndexException expected){caught=true;System.out.println("EXPECTED_CORRUPTION "+expected);}
   if(!caught)throw new AssertionError("corruption not detected");
   if(!CaptureAgent.installed.get() || CaptureAgent.calls.get()!=2 || CaptureAgent.failures.get()!=1 || CaptureAgent.reserved.get()!=0)throw new AssertionError("capture accounting invalid");
   System.out.println("SELFTEST_OK");
  }
 }
}
