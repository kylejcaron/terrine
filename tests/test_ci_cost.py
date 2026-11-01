from scripts.ci_cost import CostSummary, summarize


def test_summarize_adds_job_elapsed_and_pytest_cpu_markers():
    jobs = [
        {
            "id": 1,
            "conclusion": "success",
            "started_at": "2026-11-01T10:00:00Z",
            "completed_at": "2026-11-01T10:01:00Z",
        },
        {
            "id": 2,
            "conclusion": "success",
            "started_at": "2026-11-01T10:00:00Z",
            "completed_at": "2026-11-01T10:00:30Z",
        },
        {
            "id": 3,
            "conclusion": "skipped",
            "started_at": "2026-11-01T10:00:00Z",
            "completed_at": "2026-11-01T10:00:00Z",
        },
    ]
    logs = {
        1: "x\nterrine-pytest-cpu 10.5 1.5\nterrine-pytest-cpu 2.0 0.0\n",
        2: "no marker\n",
    }

    summary = summarize(jobs, logs)

    assert summary == CostSummary(job_seconds=90.0, pytest_cpu_seconds=14.0, jobs=2)
