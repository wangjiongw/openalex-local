import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;

public class ReadOnlyIndexCheck {
 public static void main(String[] args) throws Exception {
  if (args.length < 1 || args.length > 2) throw new IllegalArgumentException("source [isolated-copy]");
  Path target = Path.of(args[0]);
  // Caller must keep the source closed throughout. Never bypass its writer lock.
  if (args.length == 2) {
   target = Path.of(args[1]);
   Files.createDirectory(target); // Refuse reuse of a stale or shared destination.
   try (var source = new NIOFSDirectory(Path.of(args[0]))) {
    var commits = DirectoryReader.listCommits(source);
    var commit = commits.get(commits.size() - 1);
    long bytes = 0;
    for (String file : commit.getFileNames()) bytes = Math.addExact(bytes, source.fileLength(file));
    if (Files.getFileStore(target).getUsableSpace() < bytes + (50L << 30))
     throw new IllegalStateException("Insufficient space for isolated index check");
    System.out.println("COPY_COMMIT " + commit.getSegmentsFileName() + " bytes=" + bytes);
    for (String file : commit.getFileNames()) {
     Files.copy(Path.of(args[0]).resolve(file), target.resolve(file));
    }
    var after = DirectoryReader.listCommits(source);
    if (after.get(after.size() - 1).getGeneration() != commit.getGeneration())
     throw new IllegalStateException("Source commit changed during copy");
   }
  }
  try(var dir=new NIOFSDirectory(target);var check=new CheckIndex(dir)) {
   check.setThreadCount(Integer.getInteger("oa.check.threads", 1));
   check.setInfoStream(System.out);
   if(!check.checkIndex().clean)throw new IllegalStateException("Index failed integrity check");
  }
  System.out.println("INTEGRITY_OK");
 }
}
