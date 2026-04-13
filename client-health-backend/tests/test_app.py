import unittest
import os
from types import SimpleNamespace
from unittest.mock import patch
from datetime import datetime, timezone, timedelta

from fastapi import HTTPException
from fastapi.testclient import TestClient

import app.routes as routes
from app.db import get_db, init_db
from app.main import app, create_app
from app.models import BackfillWorkUnit, SyncRun
from app.security import is_loopback_client, require_internal_auth
from app.routes import (
    build_snapshot_dates,
    build_snapshot_from_merge_history,
    derive_sync_run_status,
    extract_unique_activity_users,
    extract_merge_history_entries,
    extract_merge_error_count,
    extract_sis_platform,
    get_school_exclusion_reason,
    get_new_york_snapshot_date,
    get_next_scheduled_sync_time,
    is_demo_school,
    maybe_enqueue_catchup_sync,
    maybe_enqueue_scheduled_sync,
    normalize_school_catalog,
    resolve_backfill_date_range,
    serialize_persisted_sync_run,
    select_schools_for_sync,
    serialize_sync_job,
    split_school_catalog,
    summarize_recent_merge_activity,
)

TEST_INTERNAL_API_KEY = "test-internal-key"
AUTH_HEADERS = {"X-Internal-API-Key": TEST_INTERNAL_API_KEY}


class SerializeSyncJobTests(unittest.TestCase):
    def test_serialize_sync_job_preserves_job_shape(self):
        payload = serialize_sync_job(
            {
                "jobId": "job-123",
                "status": "running",
                "snapshotDate": None,
                "schoolsProcessed": 3,
                "totalSchools": 10,
                "errors": ["sample error"],
                "timing": None,
                "error": None,
                "startedAt": "2026-04-11T12:00:00+00:00",
                "finishedAt": None,
            }
        )

        self.assertEqual(payload["jobId"], "job-123")
        self.assertEqual(payload["status"], "running")
        self.assertEqual(payload["schoolsProcessed"], 3)
        self.assertEqual(payload["totalSchools"], 10)
        self.assertEqual(payload["errors"], ["sample error"])

    def test_serialize_sync_job_marks_existing_run(self):
        payload = serialize_sync_job(
            {
                "jobId": "job-123",
                "status": "queued",
                "snapshotDate": None,
                "schoolsProcessed": 0,
                "totalSchools": 0,
                "errors": [],
                "timing": None,
                "error": None,
                "startedAt": None,
                "finishedAt": None,
            },
            already_running=True,
        )

        self.assertTrue(payload["alreadyRunning"])


class HealthcheckTests(unittest.TestCase):
    def test_healthz(self):
        async def fake_start_scheduler():
            return None

        async def fake_stop_scheduler():
            return None

        with patch("app.main.start_scheduled_sync_service", new=fake_start_scheduler), patch(
            "app.main.stop_scheduled_sync_service",
            new=fake_stop_scheduler,
        ):
            with TestClient(app) as client:
                response = client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})


class SchedulerHelperTests(unittest.IsolatedAsyncioTestCase):
    def test_get_new_york_snapshot_date_uses_local_calendar_day(self):
        self.assertEqual(
            get_new_york_snapshot_date(datetime(2026, 4, 12, 3, 30, tzinfo=timezone.utc)),
            "2026-04-11",
        )

    def test_get_next_scheduled_sync_time_preserves_new_york_local_clock_time(self):
        winter = get_next_scheduled_sync_time(datetime(2026, 1, 12, 10, 0, tzinfo=timezone.utc))
        summer = get_next_scheduled_sync_time(datetime(2026, 7, 12, 10, 0, tzinfo=timezone.utc))

        self.assertEqual((winter.hour, winter.minute), (7, 30))
        self.assertEqual((summer.hour, summer.minute), (7, 30))
        self.assertEqual(str(winter.tzinfo), "America/New_York")
        self.assertEqual(str(summer.tzinfo), "America/New_York")

    async def test_maybe_enqueue_scheduled_sync_uses_latest_only_scope(self):
        captured = {}

        def fake_enqueue_sync_job(**kwargs):
            captured.update(kwargs)
            return {
                "jobId": "scheduled-job",
                "scope": kwargs["scope"],
                "snapshotDate": kwargs["snapshot_date_override"],
            }, False

        with patch("app.routes.enqueue_sync_job", side_effect=fake_enqueue_sync_job):
            job = await maybe_enqueue_scheduled_sync(
                datetime(2026, 4, 12, 12, 0, tzinfo=timezone.utc)
            )

        self.assertEqual(job["scope"], "scheduled-bulk")
        self.assertEqual(captured["snapshot_date_override"], "2026-04-12")

    async def test_maybe_enqueue_scheduled_sync_respects_duplicate_guard(self):
        active_job = {"jobId": "active-job", "status": "running", "scope": "bulk"}

        with patch("app.routes.enqueue_sync_job", return_value=(active_job, True)) as enqueue_mock:
            job = await maybe_enqueue_scheduled_sync(
                datetime(2026, 4, 12, 12, 0, tzinfo=timezone.utc)
            )

        self.assertEqual(job, active_job)
        enqueue_mock.assert_called_once()

    async def test_catchup_enqueues_when_snapshot_is_missing(self):
        captured = {}

        def fake_enqueue_sync_job(**kwargs):
            captured.update(kwargs)
            return {
                "jobId": "catchup-job",
                "scope": kwargs["scope"],
                "snapshotDate": kwargs["snapshot_date_override"],
            }, False

        with patch("app.routes.has_successful_snapshot_for_date", return_value=False), patch(
            "app.routes.enqueue_sync_job",
            side_effect=fake_enqueue_sync_job,
        ):
            job = await maybe_enqueue_catchup_sync(
                datetime(2026, 4, 12, 12, 0, tzinfo=timezone.utc)
            )

        self.assertEqual(job["scope"], "scheduled-catchup-bulk")
        self.assertEqual(captured["snapshot_date_override"], "2026-04-12")

    async def test_catchup_skips_when_snapshot_exists(self):
        with patch("app.routes.has_successful_snapshot_for_date", return_value=True), patch(
            "app.routes.enqueue_sync_job",
        ) as enqueue_mock:
            job = await maybe_enqueue_catchup_sync(
                datetime(2026, 4, 12, 12, 0, tzinfo=timezone.utc)
            )

        self.assertIsNone(job)
        enqueue_mock.assert_not_called()


class LifespanSchedulerTests(unittest.TestCase):
    def test_lifespan_starts_and_stops_scheduler(self):
        calls = []

        async def fake_start_scheduler():
            calls.append("start")

        async def fake_stop_scheduler():
            calls.append("stop")

        with patch("app.main.start_scheduled_sync_service", new=fake_start_scheduler), patch(
            "app.main.stop_scheduled_sync_service",
            new=fake_stop_scheduler,
        ):
            with TestClient(app) as client:
                response = client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(calls, ["start", "stop"])


class MergeErrorCountTests(unittest.TestCase):
    def test_extracts_direct_total_count(self):
        self.assertEqual(extract_merge_error_count({"totalCount": 64, "content": [{}]}), 64)

    def test_extracts_nested_pagination_count(self):
        payload = {"data": {"items": [{}], "pagination": {"totalCount": 64}}}
        self.assertEqual(extract_merge_error_count(payload), 64)

    def test_falls_back_to_collection_length(self):
        payload = {"content": [{"id": 1}, {"id": 2}, {"id": 3}]}
        self.assertEqual(extract_merge_error_count(payload), 3)

    def test_extract_merge_history_entries_from_nested_content(self):
        payload = {"data": {"content": [{"id": "a"}, {"id": "b"}]}}
        self.assertEqual(extract_merge_history_entries(payload), [{"id": "a"}, {"id": "b"}])

    def test_extract_merge_history_entries_unwraps_merge_report(self):
        payload = {"items": [{"mergeReport": {"id": "a", "scheduleType": "nightly"}}]}
        self.assertEqual(
            extract_merge_history_entries(payload),
            [{"id": "a", "scheduleType": "nightly"}],
        )

    def test_build_snapshot_from_merge_history_aggregates_day(self):
        snapshot = build_snapshot_from_merge_history(
            school="bar01",
            display_name="Baruch College",
            sis_platform="PeopleSoftDirect",
            products=["integrations"],
            snapshot_date="2026-04-11",
            merge_entries=[
                {"id": "1", "scheduleType": "nightly", "status": "success"},
                {"id": "2", "scheduleType": "nightly", "status": "error"},
                {"id": "3", "scheduleType": "realtime", "status": "success"},
                {"id": "4", "scheduleType": "manual", "status": "failed"},
            ],
            active_users_24h=7,
        )
        self.assertEqual(snapshot["merges"]["nightly"]["total"], 2)
        self.assertEqual(snapshot["merges"]["nightly"]["succeeded"], 1)
        self.assertEqual(snapshot["merges"]["nightly"]["failed"], 1)
        self.assertEqual(snapshot["merges"]["nightly"]["finishedWithIssues"], 0)
        self.assertEqual(snapshot["merges"]["nightly"]["noData"], 0)
        
        self.assertEqual(snapshot["merges"]["realtime"]["total"], 1)
        self.assertEqual(snapshot["merges"]["realtime"]["succeeded"], 1)
        
        self.assertEqual(snapshot["merges"]["manual"]["failed"], 1)
        self.assertEqual(snapshot["merges"]["manual"]["finishedWithIssues"], 0)
        self.assertEqual(snapshot["mergeErrorsCount"], 2)
        self.assertEqual(snapshot["activeUsers24h"], 7)
        self.assertEqual(snapshot["sisPlatform"], "PeopleSoftDirect")

    def test_extract_unique_activity_users_from_keyed_object(self):
        payload = {
            "HWXr7RVN62eNIVLZjufZ": {"email": "a@example.com"},
            "UWE2FQEtuTZ7PRdMaSsR": {"userId": "user-b"},
            "qOl5yytaeXMYf1VTONFC": {},
        }
        self.assertEqual(
            extract_unique_activity_users(payload),
            ["a@example.com", "qOl5yytaeXMYf1VTONFC", "user-b"],
        )

    def test_summarize_recent_merge_activity_counts_last_24h(self):
        last_24h = 1000
        merge_entries = [
            {"timestampEnd": 2000, "scheduleType": "realtime", "status": "success"},
            {"timestampEnd": 2001, "scheduleType": "realtime", "status": "error"},
            {"timestampEnd": 2002, "scheduleType": "manual", "status": "success"},
            {"timestampEnd": 999, "scheduleType": "realtime", "status": "success"},
        ]
        self.assertEqual(
            summarize_recent_merge_activity(merge_entries, last_24h),
            (2, 1, 0, 0, 1, 1, 0, 0),
        )

    def test_summarize_recent_merge_activity_detects_granular_statuses(self):
        last_24h = 1000
        merge_entries = [
            {"timestampEnd": 2000, "scheduleType": "realtime", "status": "finishedWithIssues"},
            {"timestampEnd": 2001, "scheduleType": "realtime", "status": "noData"},
            {"timestampEnd": 2002, "scheduleType": "manual", "status": "finishedWithIssues"},
        ]
        self.assertEqual(
            summarize_recent_merge_activity(merge_entries, last_24h),
            (2, 0, 1, 1, 1, 0, 1, 0),
        )

    def test_extract_sis_platform_prefers_platform_name(self):
        payload = {"sisPlatform": "Banner", "integrationBroker": "Ethos"}
        self.assertEqual(extract_sis_platform(payload), "Banner")


class SchoolSelectionTests(unittest.TestCase):
    def test_bulk_sync_returns_all_schools(self):
        schools = [{"school": f"school-{index}"} for index in range(12)]
        selected = select_schools_for_sync(schools, school=None)
        self.assertEqual(len(selected), 12)
        self.assertEqual(selected[0]["school"], "school-0")

    def test_single_school_sync_returns_requested_school(self):
        schools = [{"school": "bar01"}, {"school": "abc01"}]
        selected = select_schools_for_sync(schools, school="bar01")
        self.assertEqual(selected, [{"school": "bar01"}])


class BackfillDateRangeTests(unittest.TestCase):
    def test_resolve_backfill_date_range_defaults_end_date_to_today(self):
        start_date, end_date = resolve_backfill_date_range("2026-01-01", None)
        self.assertEqual(start_date, "2026-01-01")
        self.assertGreaterEqual(end_date, "2026-01-01")

    def test_resolve_backfill_date_range_rejects_inverted_range(self):
        with self.assertRaises(Exception):
            resolve_backfill_date_range("2026-02-01", "2026-01-01")

    def test_build_snapshot_dates_is_inclusive(self):
        self.assertEqual(
            build_snapshot_dates("2026-01-01", "2026-01-03"),
            ["2026-01-01", "2026-01-02", "2026-01-03"],
        )


class BackfillTrackingTests(unittest.TestCase):
    def tearDown(self):
        init_db()
        db = get_db()
        try:
            db.query(BackfillWorkUnit).filter(BackfillWorkUnit.job_id.like("test-backfill-%")).delete(synchronize_session=False)
            db.query(SyncRun).filter(SyncRun.job_id.like("test-backfill-%")).delete(synchronize_session=False)
            db.commit()
        finally:
            db.close()

    def test_derive_sync_run_status_marks_stalled_backfill(self):
        status, detail = derive_sync_run_status(
            "running",
            scope="history-backfill-bulk",
            finished_at=None,
            last_heartbeat_at=datetime.now(timezone.utc) - timedelta(minutes=11),
            failed_units=0,
        )

        self.assertEqual(status, "stalled")
        self.assertEqual(detail, "heartbeat_stale")

    def test_serialize_persisted_sync_run_marks_completed_with_failures(self):
        sync_run = SyncRun(
            job_id="test-backfill-completed",
            scope="history-backfill-bulk",
            status="completed",
            failed_units=2,
        )

        payload = serialize_persisted_sync_run(sync_run)

        self.assertEqual(payload["status"], "completed_with_failures")

    def test_retry_failures_endpoint_requeues_only_failed_units(self):
        init_db()
        db = get_db()
        try:
            sync_run = SyncRun(
                job_id="test-backfill-retry",
                scope="history-backfill-bulk",
                status="completed",
                start_date="2026-01-01",
                end_date="2026-01-02",
                date_count=2,
                total_schools=2,
                completed_units=1,
                failed_units=1,
                attempted_at=datetime.now(timezone.utc),
            )
            db.add(sync_run)
            db.add(
                BackfillWorkUnit(
                    job_id="test-backfill-retry",
                    school="bar01",
                    display_name="Baruch College",
                    snapshot_date="2026-01-01",
                    status="completed",
                )
            )
            db.add(
                BackfillWorkUnit(
                    job_id="test-backfill-retry",
                    school="bar01",
                    display_name="Baruch College",
                    snapshot_date="2026-01-02",
                    status="failed",
                    last_error="boom",
                )
            )
            db.commit()
        finally:
            db.close()

        async def fake_start_scheduler():
            return None

        async def fake_stop_scheduler():
            return None

        async def fake_run_history_backfill_job(*args, **kwargs):
            return None

        with patch.object(routes, "_active_sync_job_id", None), patch.object(
            routes,
            "_last_job_trigger_monotonic",
            0.0,
        ), patch.dict(
            os.environ,
            {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY},
            clear=False,
        ), patch(
            "app.routes.run_history_backfill_job",
            new=fake_run_history_backfill_job,
        ), patch(
            "app.main.start_scheduled_sync_service",
            new=fake_start_scheduler,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=fake_stop_scheduler,
        ):
            with TestClient(app) as client:
                response = client.post(
                    "/api/client-health/history/backfill/test-backfill-retry/retry-failures",
                    headers=AUTH_HEADERS,
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["jobId"], "test-backfill-retry")


class BackfillEndpointTests(unittest.TestCase):
    def test_history_backfill_accepts_bulk_date_range(self):
        async def fake_run_history_backfill_job(*args, **kwargs):
            return None

        async def fake_start_scheduler():
            return None

        async def fake_stop_scheduler():
            return None

        with patch.object(routes, "_active_sync_job_id", None), patch.object(
            routes,
            "_last_job_trigger_monotonic",
            0.0,
        ), patch.dict(routes._sync_jobs, {}, clear=True), patch.dict(
            os.environ,
            {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY},
            clear=False,
        ), patch(
            "app.routes.run_history_backfill_job",
            new=fake_run_history_backfill_job,
        ), patch(
            "app.main.start_scheduled_sync_service",
            new=fake_start_scheduler,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=fake_stop_scheduler,
        ):
            with TestClient(app) as client:
                response = client.post(
                    "/api/client-health/history/backfill",
                    params={"startDate": "2026-01-01"},
                    headers=AUTH_HEADERS,
                )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["scope"], "history-backfill-bulk")
        self.assertEqual(payload["startDate"], "2026-01-01")
        self.assertIn("endDate", payload)

    def test_history_backfill_accepts_single_school_date_range(self):
        async def fake_run_history_backfill_job(*args, **kwargs):
            return None

        async def fake_start_scheduler():
            return None

        async def fake_stop_scheduler():
            return None

        with patch.object(routes, "_active_sync_job_id", None), patch.object(
            routes,
            "_last_job_trigger_monotonic",
            0.0,
        ), patch.dict(routes._sync_jobs, {}, clear=True), patch.dict(
            os.environ,
            {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY},
            clear=False,
        ), patch(
            "app.routes.run_history_backfill_job",
            new=fake_run_history_backfill_job,
        ), patch(
            "app.main.start_scheduled_sync_service",
            new=fake_start_scheduler,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=fake_stop_scheduler,
        ):
            with TestClient(app) as client:
                response = client.post(
                    "/api/client-health/history/backfill",
                    params={
                        "school": "bar01",
                        "startDate": "2026-01-01",
                        "endDate": "2026-01-07",
                    },
                    headers=AUTH_HEADERS,
                )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["scope"], "history-backfill-single")
        self.assertEqual(payload["school"], "bar01")
        self.assertEqual(payload["dateCount"], 7)


class SchoolExclusionTests(unittest.TestCase):
    def test_normalize_school_catalog_keeps_raw_schools(self):
        payload = [
            {"_id": "bar01", "displayName": "Baruch College", "products": ["integrations"]},
            {"_id": "demo01", "displayName": "Demo School", "products": []},
        ]

        schools = normalize_school_catalog(payload)

        self.assertEqual([school["school"] for school in schools], ["bar01", "demo01"])

    def test_split_school_catalog_separates_default_and_manual_exclusions(self):
        included, excluded = split_school_catalog(
            [
                {"school": "bar01", "displayName": "Baruch College", "products": []},
                {"school": "demo01", "displayName": "Demo School", "products": []},
                {"school": "bcc01", "displayName": "Bronx Community College", "products": []},
            ],
            {"bcc01"},
        )

        self.assertEqual([school["school"] for school in included], ["bar01"])
        self.assertEqual(
            {school["school"]: school["reason"] for school in excluded},
            {
                "demo01": "Matches excluded term: demo",
                "bcc01": "Manually excluded in Operations",
            },
        )

    def test_get_school_exclusion_reason_prefers_manual_override(self):
        self.assertEqual(
            get_school_exclusion_reason("demo01", "Demo School", {"demo01"}),
            "Manually excluded in Operations",
        )


class InternalAuthTests(unittest.IsolatedAsyncioTestCase):
    async def test_loopback_client_is_allowed_without_configured_key(self):
        request = SimpleNamespace(url=SimpleNamespace(path="/api/schools"), client=SimpleNamespace(host="127.0.0.1"))

        with patch.dict(os.environ, {"INTERNAL_API_KEY": "", "VITE_INTERNAL_API_KEY": ""}, clear=False):
            await require_internal_auth(request, x_internal_api_key=None)

    async def test_remote_client_is_rejected_without_configured_key(self):
        request = SimpleNamespace(url=SimpleNamespace(path="/api/schools"), client=SimpleNamespace(host="10.0.0.5"))

        with patch.dict(os.environ, {"INTERNAL_API_KEY": "", "VITE_INTERNAL_API_KEY": ""}, clear=False):
            with self.assertRaises(HTTPException) as exc:
                await require_internal_auth(request, x_internal_api_key=None)

        self.assertEqual(exc.exception.status_code, 503)

    def test_detects_loopback_client(self):
        request = SimpleNamespace(client=SimpleNamespace(host="localhost"))
        self.assertTrue(is_loopback_client(request))


class DemoSchoolTests(unittest.TestCase):

    def test_detects_demo_school_by_school_id(self):
        self.assertTrue(is_demo_school("demo_baruch", "Baruch Demo"))

    def test_detects_demo_school_by_display_name(self):
        self.assertTrue(is_demo_school("bar01", "Baruch Demo Tenant"))

    def test_non_demo_school_is_not_filtered(self):
        self.assertFalse(is_demo_school("bar01", "Baruch College"))


class ApiSecurityTests(unittest.TestCase):
    def test_protected_api_rejects_missing_internal_api_key(self):
        with patch.dict(os.environ, {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY}, clear=False), patch(
            "app.main.start_scheduled_sync_service",
            new=self._async_noop,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=self._async_noop,
        ):
            with TestClient(app) as client:
                response = client.get("/api/client-health/sync-runs")

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"]["code"], "invalid_internal_api_key")

    def test_protected_api_accepts_valid_internal_api_key(self):
        with patch.dict(os.environ, {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY}, clear=False), patch(
            "app.main.start_scheduled_sync_service",
            new=self._async_noop,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=self._async_noop,
        ):
            with TestClient(app) as client:
                response = client.get("/api/client-health/sync-runs", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200)
        self.assertIn("syncRuns", response.json())

    def test_active_users_returns_structured_upstream_error(self):
        with patch.dict(os.environ, {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY}, clear=False), patch(
            "app.routes.api_get",
            side_effect=RuntimeError("upstream down"),
        ), patch(
            "app.main.start_scheduled_sync_service",
            new=self._async_noop,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=self._async_noop,
        ):
            with TestClient(app) as client:
                response = client.get(
                    "/api/client-health/active-users",
                    params={"school": "bar01"},
                    headers=AUTH_HEADERS,
                )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["detail"]["code"], "active_users_lookup_failed")

    def test_allowed_origin_receives_cors_headers(self):
        with patch.dict(
            os.environ,
            {
                "INTERNAL_API_KEY": TEST_INTERNAL_API_KEY,
                "ALLOWED_ORIGINS": "http://localhost:5173",
            },
            clear=False,
        ), patch(
            "app.main.start_scheduled_sync_service",
            new=self._async_noop,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=self._async_noop,
        ):
            with TestClient(create_app()) as client:
                response = client.options(
                    "/api/client-health/sync-runs",
                    headers={
                        "Origin": "http://localhost:5173",
                        "Access-Control-Request-Method": "GET",
                        "Access-Control-Request-Headers": "X-Internal-API-Key",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("access-control-allow-origin"), "http://localhost:5173")

    def test_unknown_origin_does_not_receive_cors_allow_origin_header(self):
        with patch.dict(
            os.environ,
            {
                "INTERNAL_API_KEY": TEST_INTERNAL_API_KEY,
                "ALLOWED_ORIGINS": "http://localhost:5173",
            },
            clear=False,
        ), patch(
            "app.main.start_scheduled_sync_service",
            new=self._async_noop,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=self._async_noop,
        ):
            with TestClient(create_app()) as client:
                response = client.options(
                    "/api/client-health/sync-runs",
                    headers={
                        "Origin": "http://evil.example",
                        "Access-Control-Request-Method": "GET",
                        "Access-Control-Request-Headers": "X-Internal-API-Key",
                    },
                )

        self.assertIsNone(response.headers.get("access-control-allow-origin"))

    @staticmethod
    async def _async_noop():
        return None


if __name__ == "__main__":
    unittest.main()
