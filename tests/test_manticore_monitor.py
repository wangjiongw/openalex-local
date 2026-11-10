from openalex_api.manticore_monitor import summarize


def sample(at, count, phase='building', alive=True):
    return dict(at=at, count=count, phase=phase, builder_alive=alive)


def test_rate_uses_real_elapsed_time_and_reports_partial_window():
    report = summarize([sample(0, 100), sample(60, 700)])
    assert report['rates']['10']['rows_per_second'] == 10
    assert report['rates']['10']['observed_seconds'] == 60
    assert summarize([sample(0, 0)])['rates']['5']['rows_per_second'] is None


def test_sealing_is_not_stalled_writing():
    report = summarize([sample(t, 100, 'sealing') for t in range(0, 601, 60)])
    assert not report['alerts']
    assert report['rates']['10']['includes_nonwriting_phase']
    report = summarize([sample(t, 100) for t in range(0, 601, 60)])
    assert 'no_writing_progress_for_9_minutes' in report['alerts']


def test_failure_and_publication_are_distinct_from_dead_builder():
    assert summarize([sample(0, 100, alive=False)])['alerts'] == ['builder_not_running']
    assert summarize([sample(0, 100, 'published', False)])['alerts'] == []
    assert summarize([sample(0, 100, 'failed', False)])['alerts'] == ['failed']
