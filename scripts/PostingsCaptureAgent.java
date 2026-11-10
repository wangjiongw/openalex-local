import java.lang.instrument.*;
import java.security.ProtectionDomain;
import java.nio.file.*;
import java.lang.reflect.Array;
import java.util.concurrent.atomic.AtomicInteger;
import jdk.internal.org.objectweb.asm.*;
import jdk.internal.org.objectweb.asm.commons.AdviceAdapter;
import jdk.internal.org.objectweb.asm.commons.Method;

/** Captures merge context at the outer boundary, without instrumenting ordinal hot getters. */
public class PostingsCaptureAgent {
    static final AtomicInteger incidents = new AtomicInteger();
    static void writeProbe(String property, String label) {
        try {
            String configured=System.getProperty(property);
            if(configured==null)throw new IllegalStateException("missing "+property);
            Path dir=Path.of(configured);Files.createDirectories(dir);
            Path probe=dir.resolve("agent-write-probe.txt");
            Files.writeString(probe,"pid="+ProcessHandle.current().pid()+" label="+label+"\n",
                StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.WRITE);
            System.err.println("POSTINGS_CAPTURE_WRITE_PROBE "+label+" "+probe);
        } catch(Throwable error) {
            System.err.println("POSTINGS_CAPTURE_WRITE_PROBE_FAILED "+label+" "+error);
        }
    }
    public static void premain(String args, Instrumentation instrumentation) {
        writeProbe("oa.capture.dir","checksum");
        writeProbe("oa.merge.capture","merge");
        PostingsChecksumCapture.premain(args, instrumentation);
        instrumentation.addTransformer(new ClassFileTransformer() {
            public byte[] transform(ClassLoader loader, String name, Class<?> type, ProtectionDomain domain, byte[] bytes) {
                if (!name.equals("org/apache/lucene/codecs/DocValuesConsumer") && !name.equals("org/apache/lucene/codecs/FieldsConsumer") && !name.equals("org/apache/lucene/codecs/lucene103/Lucene103PostingsReader")) return null;
                try {
                    ClassReader reader = new ClassReader(bytes);
                    ClassWriter writer = new ClassWriter(reader, ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
                    reader.accept(new ClassVisitor(Opcodes.ASM8, writer) {
                        public MethodVisitor visitMethod(int access, String method, String descriptor, String signature, String[] exceptions) {
                            MethodVisitor delegate = super.visitMethod(access, method, descriptor, signature, exceptions);
                            if ((!method.startsWith("merge") && !method.equals("checkIntegrity")) || (access & (Opcodes.ACC_ABSTRACT|Opcodes.ACC_NATIVE)) != 0) return delegate;
                            return new AdviceAdapter(Opcodes.ASM8, delegate, access, method, descriptor) {
                                Label start = new Label(), end = new Label(), handler = new Label();
                                protected void onMethodEnter() { mark(start); }
                                public void visitMaxs(int stack, int locals) {
                                    mark(end);visitTryCatchBlock(start, end, handler, "java/lang/Throwable");mark(handler);
                                    int error = newLocal(Type.getType(Throwable.class));storeLocal(error);
                                    push(name+"."+method);if((access & Opcodes.ACC_STATIC)==0)loadThis();else visitInsn(Opcodes.ACONST_NULL);loadArgArray();loadLocal(error);
                                    invokeStatic(Type.getType(PostingsCaptureAgent.class), new Method("failure", "(Ljava/lang/String;Ljava/lang/Object;[Ljava/lang/Object;Ljava/lang/Throwable;)V"));
                                    loadLocal(error);throwException();super.visitMaxs(stack, locals);
                                }
                            };
                        }
                    }, ClassReader.EXPAND_FRAMES);
                    System.err.println("POSTINGS_CAPTURE_TRANSFORMED "+name);
                    return writer.toByteArray();
                } catch (Throwable error) {
                    System.err.println("POSTINGS_CAPTURE_TRANSFORM_FAILED " + error);
                    return null;
                }
            }
        });
        System.err.println("POSTINGS_CAPTURE_INSTALLED");
    }
    static void describe(StringBuilder out, Object object) throws Exception {
        if (object == null) { out.append("null\n");return; }
        out.append(object.getClass().getName()).append('\n');
        if (object.getClass().getName().equals("org.apache.lucene.index.FieldInfo")) {
            for (String name : new String[]{"name", "number"})
                out.append(name).append('=').append(object.getClass().getField(name).get(object)).append('\n');
        }
        if(object.getClass().getName().endsWith("PostingsReader")) {
            for(java.lang.reflect.Field field:object.getClass().getDeclaredFields()) {
                if(org.apache.lucene.store.IndexInput.class.isAssignableFrom(field.getType())) {
                    try { field.setAccessible(true);out.append(field.getName()).append('=').append(field.get(object)).append('\n'); }
                    catch(Throwable e){out.append("input unavailable\n");}
                }
            }
        }
        if (!object.getClass().getName().equals("org.apache.lucene.index.MergeState")) return;
        for (String name : new String[]{"segmentInfo", "maxDocs", "needsIndexSort", "fieldInfos", "docValuesProducers", "fieldsProducers"}) {
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
    public static void failure(String method, Object receiver, Object[] args, Throwable error) {
        int n=incidents.incrementAndGet();if(n>16)return;
        try {
            Path dir=Path.of(System.getProperty("oa.merge.capture"));Files.createDirectories(dir);
            StringBuilder out=new StringBuilder("method="+method+"\nthread="+Thread.currentThread().getName()+"\nerror="+error+"\n");
            describe(out,receiver);
            for(Object arg:args)describe(out,arg);
            for(StackTraceElement frame:error.getStackTrace())out.append(frame).append('\n');
            Files.writeString(dir.resolve("merge-"+ProcessHandle.current().pid()+"-"+n+".txt"),out.toString(),StandardOpenOption.CREATE_NEW);
            System.err.println("POSTINGS_CAPTURE_FAILURE " + n);
        } catch(Throwable capture) { System.err.println("POSTINGS_CAPTURE_WRITE_FAILED "+capture); }
    }
}
