import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.lucene.store.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class StorageProbe {
 public static void main(String[] args) throws Exception {
  Path path=Path.of(args[0]); String mode=args[1]; int each=Integer.parseInt(args[2]);
  Files.createDirectories(path);
  try(Directory dir=mode.equals("mmap")?new MMapDirectory(path):new NIOFSDirectory(path)) {
   IndexWriterConfig cfg=new IndexWriterConfig(new StandardAnalyzer());
   cfg.setOpenMode(IndexWriterConfig.OpenMode.CREATE); cfg.setRAMBufferSizeMB(16);
   try(IndexWriter w=new IndexWriter(dir,cfg)) {
    ExecutorService pool=Executors.newFixedThreadPool(4); List<Future<?>> futures=new ArrayList<>();
    for(int t=0;t<4;t++){ final int worker=t; futures.add(pool.submit(()->{
     try { for(int i=0;i<each;i++){
      List<Document> block=new ArrayList<>();
      for(int j=0;j<4;j++) {Document d=new Document();String id=worker+"-"+i+"-"+j;
       d.add(new StringField("id",id,Field.Store.YES));d.add(new StoredField("raw",("stored record "+id+" 气候变化 ").repeat(30)));
       d.add(new TextField("title","Bibliographic record "+id+" kidney climate mechanism",Field.Store.NO));
       d.add(new NumericDocValuesField("year",1900+i%126));block.add(d);}
      w.addDocuments(block);if(i%1000==0)w.flush();
     }}catch(Exception e){throw new RuntimeException(e);}
    }));}
    for(Future<?> f:futures)f.get();pool.shutdown();w.commit();w.forceMerge(2);w.commit();
   }
   try(CheckIndex check=new CheckIndex(dir)){check.setInfoStream(System.out);if(!check.checkIndex().clean)throw new IllegalStateException("CheckIndex failed");}
   try(DirectoryReader r=DirectoryReader.open(dir)){if(r.numDocs()!=each*16)throw new IllegalStateException("wrong document count");}
  }
  System.out.println("PASS mode="+mode+" documents="+each*16);
 }
}
