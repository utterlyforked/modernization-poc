"""Writes test-results/summary.json (stable, flat schema) alongside the JUnit XML."""
import json
import os

RESULTS_DIR = os.environ.get("TEST_RESULTS_DIR", "test-results")
_results = []


def pytest_runtest_logreport(report):
    if report.when == "call" or (report.when == "setup" and report.outcome != "passed"):
        _results.append({
            "id": report.nodeid,
            "outcome": report.outcome,
            "duration_s": round(report.duration, 3),
            "message": str(report.longrepr.reprcrash.message) if report.failed and hasattr(report.longrepr, "reprcrash") else None,
        })


def pytest_sessionfinish(session, exitstatus):
    counts = {}
    for r in _results:
        counts[r["outcome"]] = counts.get(r["outcome"], 0) + 1
    summary = {
        "status": "passed" if exitstatus == 0 else "failed",
        "total": len(_results),
        "passed": counts.get("passed", 0),
        "failed": counts.get("failed", 0),
        "skipped": counts.get("skipped", 0),
        "tests": _results,
    }
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(os.path.join(RESULTS_DIR, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSUMMARY status={summary['status']} total={summary['total']} "
          f"passed={summary['passed']} failed={summary['failed']} skipped={summary['skipped']}")
