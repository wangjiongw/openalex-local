"""Regression: checking a committed copy must preserve the source writer lock."""

import os
import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest
from openalex_api.config import ROOT
from openalex_api.storage import assert_index_closed


def test_copy_requires_closed_source():
    b = Mock()
    b.os.return_value = [{"index": "example", "status": "open"}]
    with pytest.raises(RuntimeError, match="not closed"):
        assert_index_closed(b, "example")
    b.os.return_value = []
    with pytest.raises(RuntimeError, match="not closed"):
        assert_index_closed(b, "example")


@pytest.mark.skipif(
    os.environ.get("OA_LIVE_TESTS") != "1", reason="requires bundled Lucene"
)
def test_committed_copy_with_writer_lock():
    import tempfile

    base = ROOT / "var/test-fixtures"
    base.mkdir(exist_ok=True)
    java = ROOT / "var/engines/opensearch-3.3.2/jdk/bin"
    lib = str(ROOT / "var/engines/opensearch-3.3.2/lib/*")
    with tempfile.TemporaryDirectory(prefix="lock-check-", dir=base) as tmp:
        p = Path(tmp)
        source = p / "LockCheckRegression.java"
        source.write_text("""
import java.nio.file.*;
import org.apache.lucene.store.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
public class LockCheckRegression {
 public static void main(String[] args) throws Exception {
  Path root=Path.of(args[0]);
  try(var d=new NIOFSDirectory(root.resolve("source"));
      var w=new IndexWriter(d,new IndexWriterConfig())) {
   var doc=new Document(); doc.add(new StringField("id","one",Field.Store.YES));
   w.addDocument(doc);w.commit();
   boolean locked=false;
   try { ReadOnlyIndexCheck.main(new String[]{root.resolve("source").toString()}); }
   catch(LockObtainFailedException ex) { locked=true; }
   if(!locked)throw new AssertionError("source lock not enforced");
   ReadOnlyIndexCheck.main(new String[]{root.resolve("source").toString(),root.resolve("copy").toString()});
   try(var reader=DirectoryReader.open(d)) {
    if(reader.numDocs()!=1)throw new AssertionError("source changed");
   }
   // A corrupt committed segment must still fail; copying cannot mask corruption.
   try(var files=Files.list(root.resolve("copy"))) {
    Path victim=files.filter(f->f.toString().endsWith(".cfs")).findFirst().orElseThrow();
    Files.write(victim,new byte[]{0},StandardOpenOption.TRUNCATE_EXISTING);
   }
   boolean corrupt=false;
   try { ReadOnlyIndexCheck.main(new String[]{root.resolve("copy").toString()}); }
   catch(Exception ex) { corrupt=true; }
   if(!corrupt)throw new AssertionError("corruption accepted");
  }
 }
}
""")
        subprocess.run(
            [
                str(java / "javac"),
                "-cp",
                lib,
                "-d",
                tmp,
                str(ROOT / "scripts/ReadOnlyIndexCheck.java"),
                str(source),
            ],
            check=True,
        )
        subprocess.run(
            [
                str(java / "java"),
                "-Xmx1g",
                "-cp",
                tmp + ":" + lib,
                "LockCheckRegression",
                tmp,
            ],
            check=True,
            capture_output=True,
            text=True,
        )


def test_failed_copy_is_preserved_with_original_exception(tmp_path, monkeypatch):
    monkeypatch.setattr("openalex_api.storage.diagnostics.incident", lambda *a, **k: "/test-evidence")
    from openalex_api.storage import index_check_copy
    import json

    source = tmp_path / "source"
    source.mkdir()
    (source / "segment").write_bytes(b"original")
    logfile = tmp_path / "check.log"
    logfile.write_text("checksum failed")
    failure = RuntimeError("integrity failure")
    with pytest.raises(RuntimeError) as ex:
        with index_check_copy(tmp_path, source, logfile) as scratch:
            (scratch / "index").mkdir()
            (scratch / "index/segment").write_bytes(b"different")
            raise failure
    assert ex.value is failure
    report = json.loads((scratch / "failure.json").read_text())
    assert (
        report["files"]["segment"]["source_sha256"]
        != report["files"]["segment"]["copy_sha256"]
    )
    assert (scratch / "check.log").read_text() == "checksum failed"
    assert (scratch / "index/segment").read_bytes() == b"different"
    assert (source / "segment").read_bytes() == b"original"


def test_successful_copy_is_removed(tmp_path):
    from openalex_api.storage import index_check_copy

    with index_check_copy(tmp_path, tmp_path, tmp_path / "check.log") as scratch:
        (scratch / "test").write_text("ok")
    assert not scratch.exists()


def test_resource_status_survives_disappearing_state_file(tmp_path, monkeypatch):
    from openalex_api import manage

    run = tmp_path / "var/run"
    run.mkdir(parents=True)
    (run / "disk-usage.json").write_text("{}")

    def disappeared(path, child=False):
        path.unlink(missing_ok=True)
        return None

    monkeypatch.setattr(manage, "ROOT", tmp_path)
    monkeypatch.setattr(manage, "identity", disappeared)
    monkeypatch.setattr(manage, "config", lambda: {"memory_gib": 256})
    assert manage.resource_status()["services"] == {}
