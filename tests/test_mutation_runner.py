from scripts.mutate import read_report


def test_report_counts_started_tests_but_not_collection_skips(tmp_path):
    report = tmp_path / "report.xml"
    report.write_text("""<testsuites><testsuite>
        <testcase file="tests/test_values.py" name="test_pass" />
        <testcase file="tests/test_values.py" name="test_runtime_skip">
            <skipped type="pytest.skip" message="collection skipped" />
        </testcase>
        <testcase file="tests/test_values.py" name="test_failed">
            <failure message="assertion failed" />
        </testcase>
        <testcase file="tests/test_values.py" name="test_broken">
            <error message="setup failed" />
        </testcase>
        <testcase file="tests/test_optional.py" name="tests/test_optional.py">
            <skipped message="collection skipped">optional dependency missing</skipped>
        </testcase>
    </testsuite></testsuites>""")

    seen, failed = read_report(report)

    assert seen == {
        "tests/test_values.py::test_pass",
        "tests/test_values.py::test_runtime_skip",
        "tests/test_values.py::test_failed",
        "tests/test_values.py::test_broken",
    }
    assert failed == {
        "tests/test_values.py::test_failed",
        "tests/test_values.py::test_broken",
    }
