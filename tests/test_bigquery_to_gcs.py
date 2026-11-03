"""Exercise extract retries through SDK clients without network or credentials."""

import importlib.util
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from google.api_core.exceptions import BadRequest, Conflict
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

spec = importlib.util.spec_from_file_location(
    "genetl_gcp_extract", Path(__file__).parents[1] / "src" / "etl_tools" / "gcp.py"
)
gcp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gcp)


@pytest.fixture
def exports(monkeypatch):
    client = bigquery.Client(
        project="billing-project", credentials=AnonymousCredentials()
    )
    state = SimpleNamespace(
        client=client, jobs={}, accepted=0, lose_response=False, result_error=None
    )
    lock = threading.Lock()

    def accept(job, **kwargs):
        with lock:
            if job.job_id in state.jobs:
                raise Conflict("already accepted")
            job._properties["status"] = {"state": "DONE"}
            job.result = Mock(side_effect=state.result_error)
            state.jobs[job.job_id] = job
            state.accepted += 1
            if state.lose_response:
                state.lose_response = False
                raise TimeoutError("accepted, response lost")

    monkeypatch.setattr(bigquery.ExtractJob, "_begin", accept)
    monkeypatch.setattr(
        client, "get_job", Mock(side_effect=lambda job_id, **kwargs: state.jobs[job_id])
    )
    monkeypatch.setattr(
        client,
        "get_dataset",
        Mock(return_value=SimpleNamespace(location="us-central1")),
    )
    return state


def request(exports, **overrides):
    options = dict(
        project_id="data-project",
        dataset_id="results",
        table_id="source",
        gcs_file_path="gs://example-bucket/export/output.csv",
        bq_client=exports.client,
        job_id="export_output",
        print_header=False,
    )
    options.update(overrides)
    return gcp.bigquery_to_gcs(**options)


@pytest.mark.parametrize(
    "file_format,destination_format",
    [
        ("csv", "CSV"),
        ("json", "NEWLINE_DELIMITED_JSON"),
        ("parquet", "PARQUET"),
        ("avro", "AVRO"),
    ],
)
def test_supported_formats_retry_same_job(exports, file_format, destination_format):
    options = dict(
        file_format=file_format,
        gcs_file_path=f"gs://example-bucket/output-*.{file_format}",
    )
    job = request(exports, **options)
    assert request(exports, **options) is job
    assert exports.accepted == 1
    assert job.destination_format == destination_format
    assert job.source == bigquery.TableReference.from_string(
        "data-project.results.source"
    )
    assert job.destination_uris == [options["gcs_file_path"]]
    assert job.result.call_count == 2
    exports.client.get_dataset.assert_called_with("data-project.results")
    exports.client.get_job.assert_called_once_with(
        "export_output", project="billing-project", location="us-central1"
    )


def test_concurrent_deliveries_export_once(exports):
    with ThreadPoolExecutor(max_workers=8) as executor:
        jobs = list(executor.map(lambda _: request(exports), range(16)))
    assert exports.accepted == 1
    assert all(job is jobs[0] for job in jobs)
    assert jobs[0].result.call_count == 16


def test_lost_submission_response_does_not_repeat_export(exports):
    exports.lose_response = True
    with pytest.raises(TimeoutError, match="response lost"):
        request(exports)
    job = request(exports)
    assert exports.accepted == 1
    job.result.assert_called_once()


@pytest.mark.parametrize(
    "changes",
    [
        {"project_id": "other-project"},
        {"dataset_id": "other"},
        {"table_id": "other"},
        {"gcs_file_path": "gs://example-bucket/other.csv"},
        {"file_format": "parquet"},
        {"print_header": True},
        {"compression": "GZIP"},
        {"field_delimiter": ";"},
        {"location": "EU"},
        {"labels": {"purpose": "other"}},
    ],
)
def test_reused_id_with_different_export_is_rejected(exports, changes):
    job = request(exports)
    with pytest.raises(ValueError, match="does not match"):
        request(exports, **changes)
    assert exports.accepted == 1
    job.result.assert_called_once()


@pytest.mark.parametrize(
    "path,value",
    [
        (("extract", "sourceTable", "tableId"), "other"),
        (("extract", "destinationUris"), ["gs://example-bucket/other.csv"]),
        (("extract", "printHeader"), True),
    ],
)
def test_existing_job_configuration_checked_beyond_fingerprint(exports, path, value):
    job = request(exports)
    resource = job._properties["configuration"]
    for key in path[:-1]:
        resource = resource[key]
    resource[path[-1]] = value
    with pytest.raises(ValueError, match="does not match"):
        request(exports)
    job.result.assert_called_once()


def test_remote_export_failure_propagates_on_delivery_and_retry(exports):
    exports.result_error = BadRequest("remote extract failed")
    for _ in range(2):
        with pytest.raises(BadRequest, match="remote extract failed"):
            request(exports)
    assert exports.accepted == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"job_id": ""},
        {"job_id": " "},
        {"job_id": 42},
        {"ignored_option": True},
        {"_properties": {}},
        {"to_api_repr": None},
        {"destination_format": "AVRO"},
    ],
)
def test_invalid_deterministic_options_fail_before_submit(exports, changes):
    with pytest.raises(ValueError):
        request(exports, **changes)
    assert exports.accepted == 0


def test_positional_job_id_and_location_are_forwarded(exports):
    job = gcp.bigquery_to_gcs(
        "data-project",
        "results",
        "source",
        "gs://example-bucket/output.csv",
        "csv",
        exports.client,
        False,
        "positional_export",
        "EU",
        labels={"purpose": "test"},
        field_delimiter=";",
    )
    assert job.job_id == "positional_export"
    assert job.location == "EU"
    assert job.print_header is False
    assert job.field_delimiter == ";"
    assert job.labels["purpose"] == "test"
    assert len(job.labels["genetl_extract_fingerprint"]) <= 63
    exports.client.get_dataset.assert_not_called()


@pytest.mark.parametrize("file_format", ["csv", "json", "parquet", "avro"])
def test_legacy_exports_keep_sdk_generated_ids(exports, file_format):
    first = request(exports, job_id=None, file_format=file_format, ignored_legacy=True)
    second = request(exports, job_id=None, file_format=file_format, ignored_legacy=True)
    assert first.job_id != second.job_id
    assert exports.accepted == 2
    assert not first.labels
    exports.client.get_dataset.assert_not_called()
    exports.client.get_job.assert_not_called()


def test_legacy_export_forwards_explicit_location(exports):
    job = request(exports, job_id=None, location="EU")
    assert job.location == "EU"
    exports.client.get_dataset.assert_not_called()
