import java.lang.instrument.*;
import java.security.ProtectionDomain;
import java.nio.file.*;
import java.lang.reflect.Array;
import java.util.concurrent.atomic.AtomicInteger;
import jdk.internal.org.objectweb.asm.*;
import jdk.internal.org.objectweb.asm.commons.AdviceAdapter;
import jdk.internal.org.objectweb.asm.commons.Method;

/** Captures merge context at the outer boundary, without instrumenting ordinal hot getters. */
public class MergeFailureCapture {
    static final AtomicInteger incidents = new AtomicInteger();
    static final java.util.concurrent.atomic.AtomicBoolean probed = new java.util.concurrent.atomic.AtomicBoolean();
    public static void premain(String args, Instrumentation instrumentation) {
        instrumentation.addTransformer(new ClassFileTransformer() {
            public byte[] transform(ClassLoader loader, String name, Class<?> type, ProtectionDomain domain, byte[] bytes) {
                if (!name.equals("org/apache/lucene/codecs/DocValuesConsumer") && !name.equals("org/apache/lucene/util/packed/DirectWriter")) return null;
                try {
                    ClassReader reader = new ClassReader(bytes);
                    ClassWriter writer = new ClassWriter(reader, ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
                    reader.accept(new ClassVisitor(Opcodes.ASM8, writer) {
                        public MethodVisitor visitMethod(int access, String method, String descriptor, String signature, String[] exceptions) {
                            MethodVisitor delegate = super.visitMethod(access, method, descriptor, signature, exceptions);
                            if (!(method.startsWith("merge") || (name.endsWith("DirectWriter") && method.equals("add"))) || (access & (Opcodes.ACC_ABSTRACT|Opcodes.ACC_NATIVE)) != 0) return delegate;
                            return new AdviceAdapter(Opcodes.ASM8, delegate, access, method, descriptor) {
                                Label start = new Label(), end = new Label(), handler = new Label();
                                protected void onMethodEnter() { mark(start); if(method.equals("merge")) invokeStatic(Type.getType(MergeFailureCapture.class), new Method("probe", "()V"));
                                    if(name.endsWith("DirectWriter") && Boolean.getBoolean("oa.capture.test")) invokeStatic(Type.getType(MergeFailureCapture.class), new Method("injectForTest", "()V")); }
                                public void visitMaxs(int stack, int locals) {
                                    mark(end);visitTryCatchBlock(start, end, handler, "java/lang/Throwable");mark(handler);
                                    int error = newLocal(Type.getType(Throwable.class));storeLocal(error);
                                    push(method);
                                    if(name.endsWith("DirectWriter")) { push(1);newArray(Type.getType(Object.class));dup();push(0);loadThis();arrayStore(Type.getType(Object.class)); } else loadArgArray();
                                    loadLocal(error);
                                    invokeStatic(Type.getType(MergeFailureCapture.class), new Method("failure", "(Ljava/lang/String;[Ljava/lang/Object;Ljava/lang/Throwable;)V"));
                                    loadLocal(error);throwException();super.visitMaxs(stack, locals);
                                }
                            };
                        }
                    }, ClassReader.EXPAND_FRAMES);
                    System.err.println("MERGE_CAPTURE_TRANSFORMED " + name);
                    return writer.toByteArray();
                } catch (Throwable error) {
                    System.err.println("MERGE_CAPTURE_TRANSFORM_FAILED " + error);
                    return null;
                }
            }
        });
        System.err.println("MERGE_CAPTURE_INSTALLED");
    }
    public static void injectForTest() throws java.io.IOException {
        if(!Boolean.getBoolean("oa.capture.test"))return;
        Path marker=Path.of(System.getProperty("oa.merge.capture"),"inject-once");
        if(Files.deleteIfExists(marker))throw new java.io.EOFException("OA_CAPTURE_INJECTED");
    }
    public static void probe() {
        if (!probed.compareAndSet(false,true)) return;
        try {
            Path dir=Path.of(System.getProperty("oa.merge.capture"));Files.createDirectories(dir);
            Files.writeString(dir.resolve("agent-write-probe.txt"),"actual DocValues merge pid="+ProcessHandle.current().pid()+"\n");
            System.err.println("MERGE_CAPTURE_WRITE_PROBE_OK");
        } catch(Throwable error) { throw new IllegalStateException("merge capture write probe failed",error); }
    }
    static void describe(StringBuilder out, Object object) throws Exception {
        if (object == null) { out.append("null\n");return; }
        out.append(object.getClass().getName()).append('\n');
        if (object.getClass().getName().equals("org.apache.lucene.index.FieldInfo")) {
            for (String name : new String[]{"name", "number"})
                out.append(name).append('=').append(object.getClass().getField(name).get(object)).append('\n');
        }
        if (object.getClass().getName().equals("org.apache.lucene.util.packed.DirectWriter")) {
            for(String name:new String[]{"count","numValues","bitsPerValue","off","finished"}) {
                java.lang.reflect.Field field=object.getClass().getDeclaredField(name);field.setAccessible(true);
                out.append(name).append('=').append(field.get(object)).append('\n');
            }
        }
        if (!object.getClass().getName().equals("org.apache.lucene.index.MergeState")) return;
        for (String name : new String[]{"segmentInfo", "maxDocs", "needsIndexSort", "fieldInfos", "docValuesProducers"}) {
            try {
                Object value = object.getClass().getField(name).get(object);
                out.append(name).append('=');
                if (value != null && value.getClass().isArray()) {
                    int length = Array.getLength(value);out.append("length=").append(length).append('\n');
                    for (int i=0;i<Math.min(length,256);i++) {
                        Object entry=Array.get(value,i);out.append(i).append(':');
                        if (entry instanceof Iterable<?>) {
                            int n=0;
                            for (Object field : (Iterable<?>)entry) {
                                if (++n>256)break;
                                out.append(field.getClass().getField("name").get(field)).append('#')
                                   .append(field.getClass().getField("number").get(field)).append(' ');
                            }
                        } else out.append(String.valueOf(entry));
                        out.append('\n');
                    }
                } else out.append(String.valueOf(value)).append('\n');
            } catch (Throwable e) { out.append("capture unavailable: ").append(e.getClass().getName()).append('\n'); }
        }
    }
    public static void failure(String method, Object[] args, Throwable error) {
        int n=incidents.incrementAndGet();if(n>16)return;
        try {
            Path dir=Path.of(System.getProperty("oa.merge.capture"));Files.createDirectories(dir);
            StringBuilder out=new StringBuilder("method="+method+"\nthread="+Thread.currentThread().getName()+"\nerror="+error+"\n");
            for(Object arg:args)describe(out,arg);
            for(StackTraceElement frame:error.getStackTrace())out.append(frame).append('\n');
            Files.writeString(dir.resolve("merge-"+ProcessHandle.current().pid()+"-"+n+".txt"),out.toString(),StandardOpenOption.CREATE_NEW);
            System.err.println("MERGE_CAPTURE_FAILURE " + n);
        } catch(Throwable capture) { System.err.println("MERGE_CAPTURE_WRITE_FAILED "+capture); }
    }
}
