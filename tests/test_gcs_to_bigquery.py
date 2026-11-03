"""Exercise load retries with real SDK objects and no network or credentials."""

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
    "genetl_gcp", Path(__file__).parents[1] / "src" / "etl_tools" / "gcp.py"
)
gcp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gcp)


class AtomicLoadClient:
    """Model acceptance separately from receipt of the submission response."""

    project = "billing-project"

    def __init__(self):
        self.lock = threading.Lock()
        self.jobs = {}
        self.accepted = 0
        self.lose_response = False
        self.result_error = None
        self.get_calls = []
        self.get_dataset = Mock(return_value=SimpleNamespace(location="us-central1"))

    def load_table_from_uri(self, uri, destination, *, job_id, location, job_config):
        with self.lock:
            if job_id in self.jobs:
                raise Conflict("already accepted")
            if isinstance(destination, str):
                destination = bigquery.TableReference.from_string(destination)
            configuration = job_config.to_api_repr()
            configuration["load"].update(
                sourceUris=[uri], destinationTable=destination.to_api_repr()
            )
            # REST encodes int64 options as strings; comparison must normalize.
            configuration["load"]["skipLeadingRows"] = str(
                configuration["load"]["skipLeadingRows"]
            )
            resource = {
                "configuration": configuration,
                "jobReference": {
                    "projectId": self.project,
                    "jobId": job_id,
                    "location": location,
                },
                "status": {"state": "DONE"},
            }
            job = bigquery.LoadJob.from_api_repr(resource, self)
            job.result = Mock(side_effect=self.result_error)
            self.jobs[job_id] = job
            self.accepted += 1
            if self.lose_response:
                self.lose_response = False
                raise TimeoutError("accepted, response lost")
            return job

    def get_job(self, job_id, *, project, location):
        self.get_calls.append((job_id, project, location))
        return self.jobs[job_id]


@pytest.fixture
def client():
    return AtomicLoadClient()


def request(client, **overrides):
    arguments = dict(
        bq_client=client,
        gcs_file_path="gs://example-bucket/export/output.csv",
        project_id="data-project",
        dataset_id="results",
        table_id="output",
        job_id="export_output",
        autodetect=False,
        write_disposition="WRITE_APPEND",
        allow_quoted_newlines=True,
        null_marker="\\N",
        skip_leading_rows=0,
    )
    arguments.update(overrides)
    return gcp.gcs_to_bigquery(**arguments)


def test_concurrent_deliveries_append_once_and_wait_on_same_job(client):
    with ThreadPoolExecutor(max_workers=8) as executor:
        jobs = list(executor.map(lambda _: request(client), range(16)))
    assert client.accepted == 1
    assert all(job is jobs[0] for job in jobs)
    assert jobs[0].result.call_count == 16
    assert (
        client.get_calls == [("export_output", "billing-project", "us-central1")] * 15
    )


def test_response_loss_after_acceptance_does_not_repeat_append(client):
    client.lose_response = True
    with pytest.raises(TimeoutError, match="response lost"):
        request(client)
    job = request(client)
    assert client.accepted == 1
    job.result.assert_called_once()


def test_completed_retry_normalizes_rest_numeric_strings(client):
    first = request(client)
    assert first.to_api_repr()["configuration"]["load"]["skipLeadingRows"] == "0"
    assert request(client) is first
    assert client.accepted == 1
    assert first.result.call_count == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"gcs_file_path": "gs://example-bucket/another.csv"},
        {"project_id": "other-project"},
        {"dataset_id": "other_dataset"},
        {"table_id": "other_table"},
        {"write_disposition": "WRITE_TRUNCATE"},
        {"null_marker": "NULL"},
        {"autodetect": True},
        {"skip_leading_rows": 1},
        {"labels": {"purpose": "different"}},
        {"location": "US"},
    ],
)
def test_conflicting_request_is_rejected(client, changes):
    first = request(client)
    with pytest.raises(ValueError, match="does not match"):
        request(client, **changes)
    assert client.accepted == 1
    first.result.assert_called_once()


@pytest.mark.parametrize("field,value", [("projectId", "other"), ("location", "EU")])
def test_existing_job_in_wrong_scope_is_rejected(client, field, value):
    job = request(client)
    job._properties["jobReference"][field] = value
    with pytest.raises(ValueError, match="does not match"):
        request(client)
    job.result.assert_called_once()


def test_remote_failure_propagates_on_first_delivery_and_retry(client):
    client.result_error = BadRequest("remote job failed")
    for _ in range(2):
        with pytest.raises(BadRequest, match="remote job failed"):
            request(client)
    assert client.accepted == 1


def test_csv_options_and_user_labels_are_preserved(client):
    job = request(client, labels={"purpose": "test"}, file_format=None)
    configuration = job.to_api_repr()["configuration"]
    load = configuration["load"]
    assert load["sourceFormat"] == "CSV"
    assert load["writeDisposition"] == "WRITE_APPEND"
    assert load["autodetect"] is False
    assert load["allowQuotedNewlines"] is True
    assert load["nullMarker"] == "\\N"
    assert load["skipLeadingRows"] == "0"
    assert configuration["labels"]["purpose"] == "test"
    assert len(configuration["labels"]["genetl_load_fingerprint"]) <= 63
    assert request(client, labels={"purpose": "test"}, file_format=None) is job
    client.get_dataset.assert_called_with("data-project.results")


@pytest.mark.parametrize(
    "changes",
    [
        {"job_id": ""},
        {"job_id": "   "},
        {"job_id": 42},
        {"ignored_option": True},
        {"source_format": "PARQUET"},
        {"_properties": {}},
        {"to_api_repr": None},
        {"file_format": "parquet"},
    ],
)
def test_invalid_deterministic_request_fails_before_submit(client, changes):
    with pytest.raises(ValueError):
        request(client, **changes)
    assert client.accepted == 0


def test_explicit_location_avoids_dataset_lookup(client):
    job = request(client, location="EU")
    assert request(client, location="EU") is job
    client.get_dataset.assert_not_called()
    assert job.location == "EU"
    assert client.get_calls == [("export_output", "billing-project", "EU")]


@pytest.mark.parametrize(
    "file_format,source_format",
    [
        ("csv", "CSV"),
        ("json", "NEWLINE_DELIMITED_JSON"),
        ("parquet", "PARQUET"),
        ("avro", "AVRO"),
    ],
)
def test_legacy_formats_keep_sdk_generated_job_ids(file_format, source_format):
    client = Mock()
    job = client.load_table_from_uri.return_value
    job.errors = None
    result = gcp.gcs_to_bigquery(
        "gs://example-bucket/source",
        "data-project",
        "results",
        "output",
        file_format,
        "WRITE_APPEND",
        False,
        client,
        ignored_legacy_option=True,
    )
    assert result is job
    job.result.assert_called_once()
    client.get_dataset.assert_not_called()
    client.get_job.assert_not_called()
    kwargs = client.load_table_from_uri.call_args.kwargs
    assert "job_id" not in kwargs
    assert kwargs["job_config"].source_format == source_format
    assert kwargs["job_config"].write_disposition == "WRITE_APPEND"
    assert not kwargs["job_config"].labels


def test_legacy_load_forwards_explicit_location():
    client = Mock()
    client.load_table_from_uri.return_value.errors = None
    gcp.gcs_to_bigquery(
        "gs://example-bucket/source.csv",
        "data-project",
        "results",
        "output",
        bq_client=client,
        location="EU",
    )
    assert client.load_table_from_uri.call_args.kwargs["location"] == "EU"
    client.get_dataset.assert_not_called()


@pytest.fixture
def sdk_client(monkeypatch):
    client = bigquery.Client(
        project="billing-project",
        credentials=AnonymousCredentials(),
        default_load_job_config=bigquery.LoadJobConfig(
            null_marker="NULL",
            schema=[bigquery.SchemaField("value", "STRING")],
            labels={"owner": "test"},
        ),
    )
    jobs = {}

    def accept(job, **kwargs):
        if job.job_id in jobs:
            raise Conflict("already accepted")
        job._properties["status"] = {"state": "DONE"}
        job.result = Mock()
        jobs[job.job_id] = job

    monkeypatch.setattr(bigquery.LoadJob, "_begin", accept)
    monkeypatch.setattr(client, "get_job", lambda job_id, **kwargs: jobs[job_id])
    return client


def load_with_defaults(client, **options):
    return gcp.gcs_to_bigquery(
        "gs://example-bucket/source.csv",
        "data-project",
        "results",
        "output",
        bq_client=client,
        job_id="default_options",
        location="EU",
        autodetect=False,
        write_disposition="WRITE_APPEND",
        **options,
    )


@pytest.mark.parametrize("caller_labels", [None, {"purpose": "caller"}])
def test_effective_client_defaults_and_labels_survive_retry(sdk_client, caller_labels):
    options = {} if caller_labels is None else {"labels": caller_labels}
    first = load_with_defaults(sdk_client, **options)
    assert first.null_marker == "NULL"
    assert first.schema == [bigquery.SchemaField("value", "STRING")]
    expected_labels = caller_labels or {"owner": "test"}
    assert first.labels == {
        **expected_labels,
        "genetl_load_fingerprint": first.labels["genetl_load_fingerprint"],
    }
    assert load_with_defaults(sdk_client, **options) is first
    assert sdk_client.default_load_job_config.labels == {"owner": "test"}


@pytest.mark.parametrize(
    "option,value",
    [
        ("null_marker", "missing"),
        ("schema", [bigquery.SchemaField("value", "INTEGER")]),
    ],
)
def test_changed_client_default_is_rejected(sdk_client, option, value):
    first = load_with_defaults(sdk_client)
    setattr(sdk_client.default_load_job_config, option, value)
    with pytest.raises(ValueError, match="does not match"):
        load_with_defaults(sdk_client)
    first.result.assert_called_once()


@pytest.mark.parametrize(
    "options",
    [
        {"quote_character": "'"},
        {"clustering_fields": ["value"]},
        {"destination_table_description": "Example"},
        {"destination_table_friendly_name": "Output"},
        {
            "destination_table_description": "Example",
            "destination_table_friendly_name": "Output",
        },
        {
            "hive_partitioning": bigquery.HivePartitioningOptions.from_api_repr(
                {"mode": "AUTO", "sourceUriPrefix": "gs://example-bucket/"}
            )
        },
        {
            "connection_properties": [
                bigquery.ConnectionProperty("session_id", "test-session")
            ]
        },
    ],
)
@pytest.mark.parametrize("as_default", [False, True])
def test_complex_sdk_options_survive_retry(sdk_client, options, as_default):
    if as_default:
        for key, value in options.items():
            setattr(sdk_client.default_load_job_config, key, value)
        options = {}
    first = load_with_defaults(sdk_client, **options)
    assert load_with_defaults(sdk_client, **options) is first


@pytest.mark.parametrize(
    "path,value",
    [
        (("load", "sourceUris"), ["gs://example-bucket/other.csv"]),
        (("load", "destinationTable", "tableId"), "other"),
        (("load", "nullMarker"), "missing"),
        (("load", "schema", "fields", 0, "type"), "INTEGER"),
        (("load", "schema", "fields"), []),
        (("load", "autodetect"), 0),
    ],
)
def test_existing_configuration_mismatch_rejected_with_original_fingerprint(
    sdk_client, path, value
):
    first = load_with_defaults(sdk_client)
    fingerprint = first.labels["genetl_load_fingerprint"]
    resource = first._properties["configuration"]
    for key in path[:-1]:
        resource = resource[key]
    resource[path[-1]] = value
    assert first.labels["genetl_load_fingerprint"] == fingerprint
    with pytest.raises(ValueError, match="does not match"):
        load_with_defaults(sdk_client)
    first.result.assert_called_once()


def test_nested_schema_numeric_strings_and_server_defaults(sdk_client):
    options = {
        "schema": [bigquery.SchemaField("value", "STRING", max_length=10)],
        "max_bad_records": 2,
    }
    first = load_with_defaults(sdk_client, **options)
    load = first._properties["configuration"]["load"]
    load["maxBadRecords"] = "2"
    load["schema"]["fields"][0]["maxLength"] = "10"
    load["ignoreUnknownValues"] = False
    assert load_with_defaults(sdk_client, **options) is first
    load["schema"]["fields"][0]["maxLength"] = "11"
    with pytest.raises(ValueError, match="does not match"):
        load_with_defaults(sdk_client, **options)
