import java.lang.instrument.*;
import java.security.ProtectionDomain;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.zip.CRC32;
import org.apache.lucene.store.*;
import jdk.internal.org.objectweb.asm.*;
import jdk.internal.org.objectweb.asm.commons.AdviceAdapter;
import jdk.internal.org.objectweb.asm.commons.Method;

public class CaptureAgent {
 static final AtomicLong reserved=new AtomicLong(), calls=new AtomicLong(), failures=new AtomicLong();
 static final long TOTAL=512L<<20, MAX=32L<<20;
 static final Path OUTPUT=Path.of(System.getProperty("oa.capture.dir","checksum-capture"));
 static final AtomicBoolean installed=new AtomicBoolean();
 public static void premain(String args, Instrumentation inst) {
  inst.addTransformer(new ClassFileTransformer(){
   public byte[] transform(ClassLoader loader,String name,Class<?> cls,ProtectionDomain pd,byte[] bytes) {
    if(!name.equals("org/apache/lucene/codecs/CodecUtil"))return null;
    try {
     ClassReader reader=new ClassReader(bytes);ClassWriter writer=new ClassWriter(reader,ClassWriter.COMPUTE_FRAMES|ClassWriter.COMPUTE_MAXS);
     reader.accept(new ClassVisitor(Opcodes.ASM8,writer){
      public MethodVisitor visitMethod(int access,String name,String desc,String signature,String[] exceptions) {
       MethodVisitor mv=super.visitMethod(access,name,desc,signature,exceptions);
       if(!name.equals("checksumEntireFile")||!desc.equals("(Lorg/apache/lucene/store/IndexInput;)J"))return mv;
       installed.set(true);
       return new AdviceAdapter(Opcodes.ASM8,mv,access,name,desc){
        Label start=new Label(),end=new Label(),handler=new Label();
        protected void onMethodEnter(){
         loadArg(0);invokeStatic(Type.getType(CaptureAgent.class),new Method("begin","(Lorg/apache/lucene/store/IndexInput;)Lorg/apache/lucene/store/IndexInput;"));storeArg(0);mark(start);
        }
        protected void onMethodExit(int opcode){
         if(opcode!=ATHROW){loadArg(0);invokeStatic(Type.getType(CaptureAgent.class),new Method("success","(Lorg/apache/lucene/store/IndexInput;)V"));}
        }
        public void visitMaxs(int stack,int locals){
         mark(end);visitTryCatchBlock(start,end,handler,"java/lang/Throwable");mark(handler);
         int ex=newLocal(Type.getType(Throwable.class));storeLocal(ex);loadArg(0);loadLocal(ex);
         invokeStatic(Type.getType(CaptureAgent.class),new Method("failure","(Lorg/apache/lucene/store/IndexInput;Ljava/lang/Throwable;)V"));loadLocal(ex);throwException();super.visitMaxs(stack,locals);
        }
       };
      }
     },ClassReader.EXPAND_FRAMES);
     System.err.println("CAPTURE_AGENT_TRANSFORMED checksumEntireFile");return writer.toByteArray();
    }catch(Throwable e){e.printStackTrace();return null;}
   }
  });
  Runtime.getRuntime().addShutdownHook(new Thread(()->System.err.println("CAPTURE_SUMMARY installed="+installed.get()+" calls="+calls.get()+" failures="+failures.get()+" reserved="+reserved.get())));
 }
 static class Trace {
  final String resource;final long length;final byte[] data;final BitSet seen;boolean released;
  Trace(IndexInput in){
   resource=in.toString();length=in.length();byte[] buffer=null;
   if(length<=MAX && length>0){long n=reserved.addAndGet(length);if(n<=TOTAL)buffer=new byte[(int)length];else reserved.addAndGet(-length);}
   data=buffer;seen=buffer==null?null:new BitSet(buffer.length);
  }
  void record(long position,byte[] bytes,int offset,int count){if(data!=null){System.arraycopy(bytes,offset,data,(int)position,count);seen.set((int)position,(int)position+count);}}
  void release(){if(!released){released=true;if(data!=null)reserved.addAndGet(-length);}}
 }
 static class Input extends FilterIndexInput {
  final Trace trace;final long base;
  Input(IndexInput in,Trace trace,long base){super("capture("+in+")",in);this.trace=trace;this.base=base;}
  public byte readByte()throws java.io.IOException {long p=base+in.getFilePointer();byte v=in.readByte();if(trace.data!=null){trace.data[(int)p]=v;trace.seen.set((int)p);}return v;}
  public void readBytes(byte[] b,int off,int len)throws java.io.IOException{long p=base+in.getFilePointer();in.readBytes(b,off,len);trace.record(p,b,off,len);}
  public Input clone(){return new Input(in.clone(),trace,base);}
  public IndexInput slice(String desc,long off,long len)throws java.io.IOException{return new Input(in.slice(desc,off,len),trace,base+off);}
 }
 public static IndexInput begin(IndexInput in){calls.incrementAndGet();return new Input(in,new Trace(in),0);}
 public static void success(IndexInput in){((Input)in).trace.release();}
 static String quote(String s){return "\""+s.replace("\\","\\\\").replace("\"","\\\"").replace("\n","\\n").replace("\r","\\r").replace("\t","\\t")+"\"";}
 static long software(byte[] data,int count){
  int c=-1;for(int i=0;i<count;i++){c^=data[i]&255;for(int j=0;j<8;j++)c=(c>>>1)^((c&1)!=0?0xedb88320:0);}return (~c)&0xffffffffL;
 }
 public static void failure(IndexInput in,Throwable error){
  Trace t=((Input)in).trace;failures.incrementAndGet();
  try {
   Files.createDirectories(OUTPUT);String id="failure-"+UUID.randomUUID();boolean complete=t.data!=null&&t.seen.nextClearBit(0)>=t.data.length;
   String extra="";
   if(t.data!=null){Files.write(OUTPUT.resolve(id+".bin"),t.data);extra+=",\"covered_bytes\":"+t.seen.cardinality();}
   if(complete&&t.data.length>=16){
    CRC32 crc=new CRC32();crc.update(t.data,0,t.data.length-8);long saved=0;for(int i=t.data.length-8;i<t.data.length;i++)saved=(saved<<8)|(t.data[i]&255);
    extra+=",\"software_crc32\":"+quote(Long.toHexString(software(t.data,t.data.length-8)))+",\"jvm_crc32_recomputed\":"+quote(Long.toHexString(crc.getValue()))+",\"stored_crc32\":"+quote(Long.toHexString(saved));
   }
   Files.writeString(OUTPUT.resolve(id+".json"),"{\"resource\":"+quote(t.resource)+",\"error\":"+quote(error.toString())+",\"length\":"+t.length+",\"captured\":"+(t.data!=null)+",\"complete\":"+complete+extra+"}\n");
   System.err.println("CHECKSUM_FAILURE_CAPTURE="+OUTPUT.resolve(id+".json"));
  }catch(Throwable captureError){System.err.println("CAPTURE_WRITE_FAILED "+captureError);}
  finally{t.release();}
 }
}
