import unittest
import os
import atexit
import shutil
import tempfile
from types import SimpleNamespace
from unittest.mock import patch
from datetime import datetime, timezone, timedelta

from fastapi import HTTPException
from fastapi.testclient import TestClient

TEST_DB_DIR = tempfile.mkdtemp(prefix="client-health-backend-tests-")
TEST_DB_PATH = os.path.join(TEST_DB_DIR, "client_health_test.db")
os.environ["CLIENT_HEALTH_DB_PATH"] = TEST_DB_PATH
atexit.register(lambda: shutil.rmtree(TEST_DB_DIR, ignore_errors=True))

import app.routes as routes
from app.db import get_database_url, get_db, get_default_db_path, init_db
from app.main import app, create_app
from app.models import BackfillWorkUnit, ErrorAnalysisGroup, SchedulerSettings, SchoolSnapshot, SyncRun
from app.security import is_loopback_client, require_internal_auth
from app.routes import (
    build_snapshot_dates,
    build_error_analysis_groups,
    build_resolution_hint,
    fetch_school_health_for_date,
    build_snapshot_from_merge_history,
    derive_sync_run_status,
    extract_merge_error_rows,
    extract_unique_activity_users,
    extract_merge_history_entries,
    extract_merge_error_count,
    extract_sis_platform,
    get_school_exclusion_reason,
    get_new_york_snapshot_date,
    get_next_scheduled_sync_time,
    get_scheduler_settings,
    is_demo_school,
    maybe_enqueue_catchup_sync,
    maybe_auto_resume_stalled_backfill,
    maybe_enqueue_scheduled_sync,
    normalize_school_catalog,
    normalize_error_message,
    prepare_backfill_auto_resume,
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

    def test_get_next_scheduled_sync_time_uses_custom_time(self):
        scheduled = get_next_scheduled_sync_time(
            datetime(2026, 4, 12, 12, 0, tzinfo=timezone.utc),
            sync_time="09:15",
        )

        self.assertEqual((scheduled.hour, scheduled.minute), (9, 15))

    async def test_maybe_enqueue_scheduled_sync_uses_latest_only_scope(self):
        captured = {}

        def fake_enqueue_sync_job(**kwargs):
            captured.update(kwargs)
            return {
                "jobId": "scheduled-job",
                "scope": kwargs["scope"],
                "snapshotDate": kwargs["snapshot_date_override"],
            }, False

        with patch("app.routes.load_scheduler_settings", return_value={"syncEnabled": True, "syncTime": "07:30"}), patch(
            "app.routes.enqueue_sync_job",
            side_effect=fake_enqueue_sync_job,
        ):
            job = await maybe_enqueue_scheduled_sync(
                datetime(2026, 4, 12, 12, 0, tzinfo=timezone.utc)
            )

        self.assertEqual(job["scope"], "scheduled-bulk")
        self.assertEqual(captured["snapshot_date_override"], "2026-04-12")

    async def test_maybe_enqueue_scheduled_sync_respects_duplicate_guard(self):
        active_job = {"jobId": "active-job", "status": "running", "scope": "bulk"}

        with patch("app.routes.load_scheduler_settings", return_value={"syncEnabled": True, "syncTime": "07:30"}), patch(
            "app.routes.enqueue_sync_job",
            return_value=(active_job, True),
        ) as enqueue_mock:
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

        with patch("app.routes.load_scheduler_settings", return_value={"syncEnabled": True, "syncTime": "07:30"}), patch(
            "app.routes.has_successful_snapshot_for_date",
            return_value=False,
        ), patch(
            "app.routes.enqueue_sync_job",
            side_effect=fake_enqueue_sync_job,
        ):
            job = await maybe_enqueue_catchup_sync(
                datetime(2026, 4, 12, 12, 0, tzinfo=timezone.utc)
            )

        self.assertEqual(job["scope"], "scheduled-catchup-bulk")
        self.assertEqual(captured["snapshot_date_override"], "2026-04-12")

    async def test_catchup_skips_when_snapshot_exists(self):
        with patch("app.routes.load_scheduler_settings", return_value={"syncEnabled": True, "syncTime": "07:30"}), patch(
            "app.routes.has_successful_snapshot_for_date",
            return_value=True,
        ), patch(
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


class SchedulerSettingsRouteTests(unittest.TestCase):
    def setUp(self):
        init_db()
        db = get_db()
        try:
            db.query(SchedulerSettings).delete(synchronize_session=False)
            db.commit()
        finally:
            db.close()

    def test_scheduler_settings_defaults_are_created(self):
        db = get_db()
        try:
            settings = get_scheduler_settings(db)
            self.assertTrue(settings.sync_enabled)
            self.assertEqual(settings.sync_time, "07:30")
        finally:
            db.close()

    def test_get_scheduler_settings_route(self):
        async def fake_start_scheduler():
            return None

        async def fake_stop_scheduler():
            return None

        with patch.dict(os.environ, {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY}, clear=False), patch(
            "app.main.start_scheduled_sync_service",
            new=fake_start_scheduler,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=fake_stop_scheduler,
        ):
            with TestClient(create_app()) as client:
                response = client.get("/api/client-health/scheduler-settings", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["syncTime"], "07:30")
        self.assertTrue(response.json()["syncEnabled"])

    def test_update_scheduler_settings_route(self):
        async def fake_start_scheduler():
            return None

        async def fake_stop_scheduler():
            return None

        with patch.dict(os.environ, {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY}, clear=False), patch(
            "app.main.start_scheduled_sync_service",
            new=fake_start_scheduler,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=fake_stop_scheduler,
        ), patch("app.routes.start_scheduled_sync_service", new=fake_start_scheduler), patch(
            "app.routes.stop_scheduled_sync_service",
            new=fake_stop_scheduler,
        ):
            with TestClient(create_app()) as client:
                response = client.put(
                    "/api/client-health/scheduler-settings",
                    headers=AUTH_HEADERS,
                    json={"syncEnabled": False, "syncTime": "09:15"},
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["syncTime"], "09:15")
        self.assertFalse(response.json()["syncEnabled"])


class MergeErrorCountTests(unittest.TestCase):
    def test_extracts_direct_total_count(self):
        self.assertEqual(extract_merge_error_count({"totalCount": 64, "content": [{}]}), 64)

    def test_extracts_nested_pagination_count(self):
        payload = {"data": {"items": [{}], "pagination": {"totalCount": 64}}}
        self.assertEqual(extract_merge_error_count(payload), 64)

    def test_falls_back_to_collection_length(self):
        payload = {"content": [{"id": 1}, {"id": 2}, {"id": 3}]}
        self.assertEqual(extract_merge_error_count(payload), 3)

    def test_extract_merge_error_rows_from_nested_content(self):
        payload = {"data": {"content": [{"id": "row-1"}, {"id": "row-2"}]}}
        self.assertEqual(
            extract_merge_error_rows(payload),
            [{"id": "row-1"}, {"id": "row-2"}],
        )

    def test_normalize_error_message_scrubs_volatile_values(self):
        message = "Course 202505 section 12345678 missing dependency 1cf0d6a2-1111-2222-3333-444444444444"
        normalized = normalize_error_message(message)

        self.assertIn("<num>", normalized)
        self.assertIn("<uuid>", normalized)
        self.assertNotIn("202505", normalized)

    def test_build_error_analysis_groups_clusters_similar_rows(self):
        rows = [
            {
                "entityType": "sections",
                "errorCode": "missing_course",
                "message": "Course 202505 missing dependency 123456",
                "termCode": "202505",
            },
            {
                "entityType": "sections",
                "errorCode": "missing_course",
                "message": "Course 202602 missing dependency 987654",
                "termCode": "202602",
            },
        ]

        groups = build_error_analysis_groups(
            snapshot_date="2026-04-13",
            school="bar01",
            display_name="Baruch College",
            sis_platform="Banner",
            merge_error_rows=rows,
        )

        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["count"], 2)
        self.assertEqual(groups[0]["termCodes"], ["202505", "202602"])
        self.assertEqual(
            groups[0]["signatureLabel"],
            "sections | missing_course | course <num> missing dependency <num>",
        )

    def test_build_resolution_hint_detects_missing_reference(self):
        hint = build_resolution_hint("section missing prerequisite reference", "sections", None)
        self.assertEqual(hint["bucket"], "missing_reference")

    def test_extract_merge_error_message_from_nested_original_error_body(self):
        payload = {
            "entityType": "sections",
            "errors": [
                {
                    "originalError": {
                        "error": "SIS operation was not successful",
                        "body": {
                            "errors": [
                                {
                                    "code": "Record.Not.Found",
                                    "description": "Unknown error code.",
                                    "message": "Invalid GUID 'DLS SEM Dist Lrn Sem' supplied for site.",
                                }
                            ]
                        },
                        "postType": "UpdateSection",
                    }
                }
            ],
        }

        groups = build_error_analysis_groups(
            snapshot_date="2026-04-13",
            school="bar01",
            display_name="Baruch College",
            sis_platform="Banner",
            merge_error_rows=[payload],
        )

        self.assertEqual(groups[0]["sampleMessage"], "Invalid GUID 'DLS SEM Dist Lrn Sem' supplied for site.")
        self.assertEqual(groups[0]["errorCode"], "Record.Not.Found")

    def test_extract_merge_error_message_from_string_body(self):
        payload = {
            "entityType": "sections",
            "errors": [
                {
                    "originalError": {
                        "error": "SIS operation was not successful",
                        "body": "ORA-20001: ORA-20100: ::Room is not defined as a classroom; cannot schedule section in it::",
                        "postType": "SetMeetingTimes",
                    }
                }
            ],
        }

        groups = build_error_analysis_groups(
            snapshot_date="2026-04-13",
            school="bar01",
            display_name="Baruch College",
            sis_platform="Banner",
            merge_error_rows=[payload],
        )

        self.assertIn("room is not defined as a classroom", groups[0]["normalizedMessage"])
        self.assertEqual(groups[0]["errorCode"], "SetMeetingTimes")

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
        self.assertIsNone(snapshot["mergeErrorsCount"])
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


class FetchSchoolHealthTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_school_health_keeps_nightly_no_data_out_of_failures(self):
        now_ms = int(datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)

        async def fake_api_get(path, params=None):
            if path.endswith("/integrations-hub/overview/health"):
                return {
                    "allNightlyMergesCount": 2,
                    "succeededNightlyMergesCount": 1,
                    "recentFailedMerges": [],
                }

            if path.endswith("/integrations-hub/overview/merge-errors"):
                return {"totalCount": 0}

            if path.endswith("/integrations-hub/merge-history") and params and params.get("scheduleType") == "nightly":
                return {
                    "content": [
                        {"id": "nightly-1", "scheduleType": "nightly", "status": "success"},
                        {"id": "nightly-2", "scheduleType": "nightly", "status": "noData"},
                    ]
                }

            if path.endswith("/integrations-hub/merge-history"):
                return {
                    "content": [
                        {
                            "id": "rt-1",
                            "scheduleType": "realtime",
                            "status": "success",
                            "timestampEnd": now_ms,
                        }
                    ]
                }

            if path.endswith("/userActivity"):
                return {"user-a": {"email": "user@example.com"}}

            raise AssertionError(f"Unexpected api_get call: {path} params={params}")

        with patch("app.routes.api_get", side_effect=fake_api_get), patch(
            "app.routes.fetch_school_sis_platform",
            return_value="Banner",
        ):
            snapshot = await routes.fetch_school_health(
                school="bar01",
                display_name="Baruch College",
                products=["integrations"],
                snapshot_date="2026-04-13",
            )

        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["merges"]["nightly"]["total"], 2)
        self.assertEqual(snapshot["merges"]["nightly"]["succeeded"], 1)
        self.assertEqual(snapshot["merges"]["nightly"]["noData"], 1)
        self.assertEqual(snapshot["merges"]["nightly"]["failed"], 0)
        self.assertEqual(snapshot["merges"]["nightly"]["finishedWithIssues"], 0)


class SyncJobErrorAnalysisPersistenceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        init_db()
        db = get_db()
        try:
            db.query(ErrorAnalysisGroup).filter(ErrorAnalysisGroup.school == "bar01").delete(synchronize_session=False)
            db.query(SchoolSnapshot).filter(SchoolSnapshot.school == "bar01").delete(synchronize_session=False)
            db.query(SyncRun).filter(SyncRun.job_id == "test-sync-error-groups").delete(synchronize_session=False)
            db.commit()
        finally:
            db.close()

    def tearDown(self):
        init_db()
        db = get_db()
        try:
            db.query(ErrorAnalysisGroup).filter(ErrorAnalysisGroup.school == "bar01").delete(synchronize_session=False)
            db.query(SchoolSnapshot).filter(SchoolSnapshot.school == "bar01").delete(synchronize_session=False)
            db.query(SyncRun).filter(SyncRun.job_id == "test-sync-error-groups").delete(synchronize_session=False)
            db.commit()
        finally:
            db.close()
            routes._sync_jobs.pop("test-sync-error-groups", None)
            routes._active_sync_job_id = None

    async def test_run_sync_job_replaces_same_day_error_groups_on_rerun(self):
        db = get_db()
        try:
            db.add(
                ErrorAnalysisGroup.from_dict(
                    {
                        "snapshotDate": "2026-04-13",
                        "school": "bar01",
                        "displayName": "Baruch College",
                        "sisPlatform": "Banner",
                        "entityType": "sections",
                        "errorCode": "old",
                        "signatureKey": "sig-old",
                        "normalizedMessage": "old error",
                        "sampleMessage": "Old error",
                        "count": 9,
                        "sampleErrors": [],
                        "termCodes": [],
                    }
                )
            )
            db.commit()
        finally:
            db.close()

        async def fake_list_schools():
            return {
                "schools": [
                    {
                        "school": "bar01",
                        "displayName": "Baruch College",
                        "products": [],
                    }
                ]
            }

        async def fake_fetch_school_health(school, display_name, products, snapshot_date):
            return {
                "snapshotDate": snapshot_date,
                "school": school,
                "displayName": display_name,
                "sisPlatform": "Banner",
                "products": products,
                "merges": {
                    "nightly": {"total": 1, "succeeded": 1, "failed": 0, "finishedWithIssues": 0, "noData": 0, "mergeTimeMs": 0},
                    "realtime": {"total": 0, "succeeded": 0, "failed": 0, "finishedWithIssues": 0, "noData": 0},
                    "manual": {"total": 0, "succeeded": 0, "failed": 0, "finishedWithIssues": 0, "noData": 0},
                },
                "recentFailedMerges": [],
                "mergeErrorsCount": 1,
                "activeUsers24h": 2,
                "_errorGroups": [
                    {
                        "snapshotDate": snapshot_date,
                        "school": school,
                        "displayName": display_name,
                        "sisPlatform": "Banner",
                        "entityType": "courses",
                        "errorCode": "new",
                        "signatureKey": "sig-new",
                        "normalizedMessage": "new error",
                        "sampleMessage": "New error",
                        "count": 1,
                        "sampleErrors": [{"message": "New error"}],
                        "termCodes": ["202505"],
                    }
                ],
                "_syncErrors": [],
            }

        routes._sync_jobs["test-sync-error-groups"] = {
            "jobId": "test-sync-error-groups",
            "status": "queued",
            "snapshotDate": "2026-04-13",
            "schoolsProcessed": 0,
            "totalSchools": 0,
            "errors": [],
            "timing": None,
            "error": None,
            "scope": "single-school",
            "school": "bar01",
            "startedAt": None,
            "finishedAt": None,
        }

        with patch("app.routes.list_schools", side_effect=fake_list_schools), patch(
            "app.routes.fetch_school_health",
            side_effect=fake_fetch_school_health,
        ):
            await routes.run_sync_job(
                "test-sync-error-groups",
                school="bar01",
                snapshot_date_override="2026-04-13",
            )

        db = get_db()
        try:
            persisted = (
                db.query(ErrorAnalysisGroup)
                .filter(
                    ErrorAnalysisGroup.school == "bar01",
                    ErrorAnalysisGroup.snapshot_date == "2026-04-13",
                )
                .all()
            )
            self.assertEqual(len(persisted), 1)
            self.assertEqual(persisted[0].signature_key, "sig-new")
        finally:
            db.close()


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
        with patch("app.routes.get_new_york_snapshot_date", return_value="2026-04-11"):
            start_date, end_date = resolve_backfill_date_range("2026-01-01", None)

        self.assertEqual(start_date, "2026-01-01")
        self.assertEqual(end_date, "2026-04-11")

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
            db.query(SchoolSnapshot).filter(SchoolSnapshot.school.like("test-backfill-%")).delete(synchronize_session=False)
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

    def test_serialize_persisted_sync_run_treats_naive_sqlite_timestamps_as_utc(self):
        sync_run = SyncRun(
            job_id="test-backfill-naive-time",
            scope="history-backfill-bulk",
            status="completed",
            attempted_at=datetime(2026, 4, 13, 11, 30),
            started_at=datetime(2026, 4, 13, 11, 30),
            finished_at=datetime(2026, 4, 13, 11, 31, 22, 14631),
        )

        payload = serialize_persisted_sync_run(sync_run)

        self.assertEqual(payload["attemptedAt"], "2026-04-13T11:30:00+00:00")
        self.assertEqual(payload["startedAt"], "2026-04-13T11:30:00+00:00")
        self.assertEqual(payload["finishedAt"], "2026-04-13T11:31:22.014631+00:00")

    def test_school_snapshot_to_dict_treats_naive_sqlite_timestamps_as_utc(self):
        snapshot = SchoolSnapshot(
            snapshot_date="2026-04-13",
            school="bar01",
            display_name="Baruch College",
            created_at=datetime(2026, 4, 13, 11, 30),
        )

        payload = snapshot.to_dict()

        self.assertEqual(payload["createdAt"], "2026-04-13T11:30:00+00:00")

    def test_serialize_persisted_sync_run_adds_reason_for_stalled_backfill(self):
        now = datetime(2026, 4, 13, 16, 0, tzinfo=timezone.utc)
        sync_run = SyncRun(
            job_id="test-backfill-stalled",
            scope="history-backfill-bulk",
            status="running",
            current_school="bar01",
            current_snapshot_date="2026-04-10",
            last_heartbeat_at=now - timedelta(minutes=14),
            last_progress_at=now - timedelta(minutes=22),
            error_message="merge history request timed out",
        )

        with patch("app.routes.datetime") as mock_datetime:
            mock_datetime.now.return_value = now
            mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            mock_datetime.fromisoformat = datetime.fromisoformat
            payload = serialize_persisted_sync_run(sync_run)

        self.assertEqual(payload["status"], "stalled")
        self.assertEqual(payload["statusDetail"], "heartbeat_stale")
        self.assertIn("No heartbeat for 14m", payload["failureReason"])
        self.assertIn("while processing bar01 on 2026-04-10", payload["failureReason"])
        self.assertIn("last progress 22m ago", payload["failureReason"])
        self.assertIn("last error: merge history request timed out", payload["failureReason"])

    def test_serialize_persisted_sync_run_preserves_existing_failure_reason_for_stalled_backfill(self):
        now = datetime(2026, 4, 13, 16, 0, tzinfo=timezone.utc)
        sync_run = SyncRun(
            job_id="test-backfill-stalled-explicit",
            scope="history-backfill-bulk",
            status="running",
            last_heartbeat_at=now - timedelta(minutes=14),
            failure_reason="Waiting on upstream dependency",
        )

        with patch("app.routes.datetime") as mock_datetime:
            mock_datetime.now.return_value = now
            mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            mock_datetime.fromisoformat = datetime.fromisoformat
            payload = serialize_persisted_sync_run(sync_run)

        self.assertEqual(payload["status"], "stalled")
        self.assertEqual(payload["failureReason"], "Waiting on upstream dependency")

    def test_prepare_backfill_auto_resume_adds_recovery_note(self):
        now = datetime(2026, 4, 13, 16, 0, tzinfo=timezone.utc)
        job = {
            "jobId": "test-backfill-auto-resume",
            "errors": ["older issue"],
            "failureReason": None,
            "statusDetail": "heartbeat_stale",
            "finishedAt": "2026-04-13T15:00:00+00:00",
            "error": "stale",
        }

        note = prepare_backfill_auto_resume(
            job,
            "No heartbeat for 14m; while processing bar01 on 2026-04-10",
            now=now,
        )

        self.assertIn("Auto-resume triggered", note)
        self.assertEqual(job["errors"][-1], note)
        self.assertEqual(
            job["failureReason"],
            "Recovering from stall: No heartbeat for 14m; while processing bar01 on 2026-04-10",
        )
        self.assertEqual(job["statusDetail"], "auto_resuming")
        self.assertIsNone(job["finishedAt"])
        self.assertIsNone(job["error"])

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
                schools_processed=2,
                completed_units=1,
                failed_units=1,
                current_school="bar01",
                current_snapshot_date="2026-01-02",
                status_detail="completed_with_failures",
                failure_reason="1 work unit failed",
                errors_json='["boom"]',
                failed_units_sample_json='[{"school":"bar01","snapshotDate":"2026-01-02","error":"boom"}]',
                checkpoint_state_json='{"currentSchool":"bar01","currentSnapshotDate":"2026-01-02","failedUnits":1}',
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
        payload = response.json()
        self.assertEqual(payload["jobId"], "test-backfill-retry")
        self.assertEqual(payload["completedUnits"], 1)
        self.assertEqual(payload["failedUnits"], 1)
        self.assertEqual(payload["totalSchools"], 2)
        self.assertEqual(payload["schoolsProcessed"], 2)
        self.assertEqual(payload["statusDetail"], "completed_with_failures")
        self.assertEqual(payload["failureReason"], "1 work unit failed")
        self.assertEqual(payload["errors"], ["boom"])
        self.assertEqual(
            payload["checkpointState"],
            {"currentSchool": "bar01", "currentSnapshotDate": "2026-01-02", "failedUnits": 1},
        )

    def test_build_backfill_job_from_sync_run_preserves_resume_context(self):
        sync_run = SyncRun(
            job_id="test-backfill-resume",
            school="bar01",
            scope="history-backfill-single",
            status="failed",
            snapshot_date="2026-01-02",
            schools_processed=3,
            total_schools=4,
            start_date="2026-01-01",
            end_date="2026-01-04",
            date_count=4,
            started_at=datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc),
            finished_at=datetime(2026, 1, 5, 10, 30, tzinfo=timezone.utc),
            last_heartbeat_at=datetime(2026, 1, 5, 10, 15, tzinfo=timezone.utc),
            last_progress_at=datetime(2026, 1, 5, 10, 20, tzinfo=timezone.utc),
            current_school="bar01",
            current_snapshot_date="2026-01-03",
            completed_units=2,
            failed_units=1,
            skipped_units=0,
            status_detail="failed",
            failure_reason="merge history request failed",
            failed_units_sample_json='[{"school":"bar01","snapshotDate":"2026-01-03","error":"boom"}]',
            checkpoint_state_json='{"currentSchool":"bar01","currentSnapshotDate":"2026-01-03","failedUnits":1}',
            errors_json='["boom"]',
            timing_json='{"totalSec":12.3}',
            error_message="boom",
        )

        payload = routes.build_backfill_job_from_sync_run(sync_run)

        self.assertEqual(payload["jobId"], "test-backfill-resume")
        self.assertEqual(payload["completedUnits"], 2)
        self.assertEqual(payload["failedUnits"], 1)
        self.assertEqual(payload["totalSchools"], 4)
        self.assertEqual(payload["schoolsProcessed"], 3)
        self.assertEqual(payload["statusDetail"], "failed")
        self.assertEqual(payload["failureReason"], "merge history request failed")
        self.assertEqual(payload["errors"], ["boom"])
        self.assertEqual(payload["checkpointState"]["currentSnapshotDate"], "2026-01-03")


class HistoricalBackfillFetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_school_health_for_date_uses_new_york_today_boundary(self):
        async def fake_fetch_school_health(**kwargs):
            return {"snapshotDate": kwargs["snapshot_date"], "school": kwargs["school"]}

        with patch(
            "app.routes.get_new_york_snapshot_date",
            return_value="2026-04-11",
        ), patch("app.routes.fetch_school_health", side_effect=fake_fetch_school_health) as fetch_mock:
            result = await fetch_school_health_for_date(
                school="bar01",
                display_name="Baruch College",
                products=[],
                snapshot_date="2026-04-11",
            )

        self.assertEqual(result["snapshotDate"], "2026-04-11")
        fetch_mock.assert_awaited_once()

    async def test_maybe_auto_resume_stalled_backfill_requeues_job(self):
        now = datetime.now(timezone.utc)
        sync_run = SyncRun(
            job_id="test-backfill-auto-resume-db",
            scope="history-backfill-bulk",
            status="running",
            school="bar01",
            start_date="2026-04-01",
            end_date="2026-04-10",
            date_count=10,
            attempted_at=now - timedelta(hours=1),
            last_heartbeat_at=now - timedelta(minutes=14),
            last_progress_at=now - timedelta(minutes=22),
            current_school="bar01",
            current_snapshot_date="2026-04-10",
            errors_json='["merge history request timed out"]',
        )

        init_db()
        db = get_db()
        try:
            db.add(sync_run)
            db.commit()
        finally:
            db.close()

        started_jobs: list[tuple[str, str]] = []

        def fake_start_backfill_job_task(job, *, mode="initial"):
            started_jobs.append((job["jobId"], mode))
            routes._sync_jobs[job["jobId"]] = job
            routes._active_sync_job_id = job["jobId"]

        with patch.object(routes, "_active_sync_job_id", None), patch.object(
            routes,
            "_sync_jobs",
            {},
        ), patch("app.routes.start_backfill_job_task", side_effect=fake_start_backfill_job_task):
            result = await maybe_auto_resume_stalled_backfill(now=now)

        self.assertEqual(result["jobId"], "test-backfill-auto-resume-db")
        self.assertEqual(started_jobs, [("test-backfill-auto-resume-db", "resume")])

        db = get_db()
        try:
            persisted = db.query(SyncRun).filter(SyncRun.job_id == "test-backfill-auto-resume-db").first()
            self.assertIsNotNone(persisted)
            errors = persisted.to_dict()["errors"]
            self.assertTrue(any("Auto-resume triggered" in entry for entry in errors))
            self.assertIn("Recovering from stall:", persisted.failure_reason)
        finally:
            db.query(SyncRun).filter(SyncRun.job_id == "test-backfill-auto-resume-db").delete(synchronize_session=False)
            db.commit()
            db.close()


class HistoricalBackfillWorkerTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self):
        init_db()
        db = get_db()
        try:
            db.query(BackfillWorkUnit).filter(BackfillWorkUnit.job_id.like("test-backfill-%")).delete(synchronize_session=False)
            db.query(SyncRun).filter(SyncRun.job_id.like("test-backfill-%")).delete(synchronize_session=False)
            db.query(SchoolSnapshot).filter(SchoolSnapshot.school.like("test-backfill-%")).delete(synchronize_session=False)
            db.commit()
        finally:
            db.close()

    async def test_backfill_does_not_overwrite_existing_snapshot_when_merge_history_fails(self):
        init_db()
        routes._sync_jobs["test-backfill-merge-fail"] = routes.create_backfill_job_payload(
            job_id="test-backfill-merge-fail",
            school="test-backfill-school",
            start_date="2026-01-01",
            end_date="2026-01-01",
            date_count=1,
        )
        db = get_db()
        try:
            db.add(
                SchoolSnapshot.from_dict(
                    {
                        "snapshotDate": "2026-01-01",
                        "school": "test-backfill-school",
                        "displayName": "Existing Snapshot",
                        "sisPlatform": "Banner",
                        "products": [],
                        "merges": {
                            "nightly": {"total": 1, "succeeded": 1, "failed": 0, "finishedWithIssues": 0, "noData": 0, "mergeTimeMs": 10},
                            "realtime": {"total": 1, "succeeded": 1, "failed": 0, "finishedWithIssues": 0, "noData": 0},
                            "manual": {"total": 0, "succeeded": 0, "failed": 0, "finishedWithIssues": 0, "noData": 0},
                        },
                        "recentFailedMerges": [],
                        "mergeErrorsCount": 0,
                        "activeUsers24h": 9,
                    }
                )
            )
            db.commit()
        finally:
            db.close()

        async def fake_list_schools():
            return {
                "schools": [
                    {
                        "school": "test-backfill-school",
                        "displayName": "Test Backfill School",
                        "products": [],
                    }
                ]
            }

        async def fake_api_get(path, params=None):
            if "merge-history" in path:
                raise RuntimeError("upstream merge history outage")
            if "userActivity" in path:
                return {"data": [{"email": "a@example.com"}]}
            raise AssertionError(f"Unexpected path {path}")

        with patch("app.routes.list_schools", side_effect=fake_list_schools), patch(
            "app.routes.api_get",
            side_effect=fake_api_get,
        ), patch("app.routes.fetch_school_sis_platform", return_value="Banner"):
            await routes.run_history_backfill_job(
                "test-backfill-merge-fail",
                school="test-backfill-school",
                start_date="2026-01-01",
                end_date="2026-01-01",
            )

        db = get_db()
        try:
            snapshot = (
                db.query(SchoolSnapshot)
                .filter(
                    SchoolSnapshot.school == "test-backfill-school",
                    SchoolSnapshot.snapshot_date == "2026-01-01",
                )
                .one()
            )
            sync_run = db.query(SyncRun).filter(SyncRun.job_id == "test-backfill-merge-fail").one()
            unit = db.query(BackfillWorkUnit).filter(BackfillWorkUnit.job_id == "test-backfill-merge-fail").one()
        finally:
            db.close()

        self.assertEqual(snapshot.display_name, "Existing Snapshot")
        self.assertEqual(snapshot.active_users_24h, 9)
        self.assertEqual(unit.status, "failed")
        self.assertEqual(sync_run.failed_units, 1)
        self.assertEqual(sync_run.completed_units, 0)

    async def test_backfill_does_not_overwrite_existing_snapshot_when_user_activity_fails(self):
        init_db()
        routes._sync_jobs["test-backfill-activity-fail"] = routes.create_backfill_job_payload(
            job_id="test-backfill-activity-fail",
            school="test-backfill-school",
            start_date="2026-01-02",
            end_date="2026-01-02",
            date_count=1,
        )
        db = get_db()
        try:
            db.add(
                SchoolSnapshot.from_dict(
                    {
                        "snapshotDate": "2026-01-02",
                        "school": "test-backfill-school",
                        "displayName": "Existing Snapshot",
                        "sisPlatform": "Banner",
                        "products": [],
                        "merges": {
                            "nightly": {"total": 2, "succeeded": 2, "failed": 0, "finishedWithIssues": 0, "noData": 0, "mergeTimeMs": 20},
                            "realtime": {"total": 0, "succeeded": 0, "failed": 0, "finishedWithIssues": 0, "noData": 0},
                            "manual": {"total": 0, "succeeded": 0, "failed": 0, "finishedWithIssues": 0, "noData": 0},
                        },
                        "recentFailedMerges": [],
                        "mergeErrorsCount": 0,
                        "activeUsers24h": 13,
                    }
                )
            )
            db.commit()
        finally:
            db.close()

        async def fake_list_schools():
            return {
                "schools": [
                    {
                        "school": "test-backfill-school",
                        "displayName": "Test Backfill School",
                        "products": [],
                    }
                ]
            }

        async def fake_api_get(path, params=None):
            if "merge-history" in path:
                return {"data": {"content": [{"id": "m1", "scheduleType": "nightly", "status": "success"}]}}
            if "userActivity" in path:
                raise RuntimeError("user activity timeout")
            raise AssertionError(f"Unexpected path {path}")

        with patch("app.routes.list_schools", side_effect=fake_list_schools), patch(
            "app.routes.api_get",
            side_effect=fake_api_get,
        ), patch("app.routes.fetch_school_sis_platform", return_value="Banner"):
            await routes.run_history_backfill_job(
                "test-backfill-activity-fail",
                school="test-backfill-school",
                start_date="2026-01-02",
                end_date="2026-01-02",
            )

        db = get_db()
        try:
            snapshot = (
                db.query(SchoolSnapshot)
                .filter(
                    SchoolSnapshot.school == "test-backfill-school",
                    SchoolSnapshot.snapshot_date == "2026-01-02",
                )
                .one()
            )
            sync_run = db.query(SyncRun).filter(SyncRun.job_id == "test-backfill-activity-fail").one()
            unit = db.query(BackfillWorkUnit).filter(BackfillWorkUnit.job_id == "test-backfill-activity-fail").one()
        finally:
            db.close()

        self.assertEqual(snapshot.display_name, "Existing Snapshot")
        self.assertEqual(snapshot.active_users_24h, 13)
        self.assertEqual(unit.status, "failed")
        self.assertEqual(sync_run.failed_units, 1)


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


class ClientHealthHistoryEndpointTests(unittest.TestCase):
    @staticmethod
    async def _async_noop():
        return None

    def setUp(self):
        init_db()
        db = get_db()
        try:
            db.query(SchoolSnapshot).filter(SchoolSnapshot.school == "bar01").delete(
                synchronize_session=False
            )
            db.commit()
        finally:
            db.close()

    def test_history_endpoint_returns_full_snapshot_history_when_days_omitted(self):
        init_db()
        db = get_db()
        try:
            db.add_all(
                [
                    SchoolSnapshot.from_dict(
                        {
                            "snapshotDate": "2026-01-15",
                            "school": "bar01",
                            "displayName": "Baruch College",
                            "sisPlatform": "Banner",
                            "products": [],
                            "merges": {
                                "nightly": {
                                    "total": 1,
                                    "succeeded": 1,
                                    "failed": 0,
                                    "finishedWithIssues": 0,
                                    "noData": 0,
                                    "mergeTimeMs": 0,
                                },
                                "realtime": {
                                    "total": 0,
                                    "succeeded": 0,
                                    "failed": 0,
                                    "finishedWithIssues": 0,
                                    "noData": 0,
                                },
                                "manual": {
                                    "total": 0,
                                    "succeeded": 0,
                                    "failed": 0,
                                    "finishedWithIssues": 0,
                                    "noData": 0,
                                },
                            },
                            "recentFailedMerges": [],
                            "mergeErrorsCount": 1,
                            "activeUsers24h": 2,
                        }
                    ),
                    SchoolSnapshot.from_dict(
                        {
                            "snapshotDate": "2026-04-12",
                            "school": "bar01",
                            "displayName": "Baruch College",
                            "sisPlatform": "Banner",
                            "products": [],
                            "merges": {
                                "nightly": {
                                    "total": 1,
                                    "succeeded": 1,
                                    "failed": 0,
                                    "finishedWithIssues": 0,
                                    "noData": 0,
                                    "mergeTimeMs": 0,
                                },
                                "realtime": {
                                    "total": 0,
                                    "succeeded": 0,
                                    "failed": 0,
                                    "finishedWithIssues": 0,
                                    "noData": 0,
                                },
                                "manual": {
                                    "total": 0,
                                    "succeeded": 0,
                                    "failed": 0,
                                    "finishedWithIssues": 0,
                                    "noData": 0,
                                },
                            },
                            "recentFailedMerges": [],
                            "mergeErrorsCount": 0,
                            "activeUsers24h": 3,
                        }
                    ),
                ]
            )
            db.commit()
        finally:
            db.close()

        with patch.dict(os.environ, {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY}, clear=False), patch(
            "app.main.start_scheduled_sync_service",
            new=self._async_noop,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=self._async_noop,
        ):
            with TestClient(app) as client:
                response = client.get("/api/client-health/history", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [snapshot["snapshotDate"] for snapshot in response.json()["snapshots"]],
            ["2026-01-15", "2026-04-12"],
        )

    def test_history_endpoint_still_supports_explicit_day_windows(self):
        init_db()
        db = get_db()
        try:
            db.add_all(
                [
                    SchoolSnapshot.from_dict(
                        {
                            "snapshotDate": "2026-04-01",
                            "school": "bar01",
                            "displayName": "Baruch College",
                            "sisPlatform": "Banner",
                            "products": [],
                            "merges": {
                                "nightly": {
                                    "total": 1,
                                    "succeeded": 1,
                                    "failed": 0,
                                    "finishedWithIssues": 0,
                                    "noData": 0,
                                    "mergeTimeMs": 0,
                                },
                                "realtime": {
                                    "total": 0,
                                    "succeeded": 0,
                                    "failed": 0,
                                    "finishedWithIssues": 0,
                                    "noData": 0,
                                },
                                "manual": {
                                    "total": 0,
                                    "succeeded": 0,
                                    "failed": 0,
                                    "finishedWithIssues": 0,
                                    "noData": 0,
                                },
                            },
                            "recentFailedMerges": [],
                            "mergeErrorsCount": 1,
                            "activeUsers24h": 2,
                        }
                    ),
                    SchoolSnapshot.from_dict(
                        {
                            "snapshotDate": "2026-04-12",
                            "school": "bar01",
                            "displayName": "Baruch College",
                            "sisPlatform": "Banner",
                            "products": [],
                            "merges": {
                                "nightly": {
                                    "total": 1,
                                    "succeeded": 1,
                                    "failed": 0,
                                    "finishedWithIssues": 0,
                                    "noData": 0,
                                    "mergeTimeMs": 0,
                                },
                                "realtime": {
                                    "total": 0,
                                    "succeeded": 0,
                                    "failed": 0,
                                    "finishedWithIssues": 0,
                                    "noData": 0,
                                },
                                "manual": {
                                    "total": 0,
                                    "succeeded": 0,
                                    "failed": 0,
                                    "finishedWithIssues": 0,
                                    "noData": 0,
                                },
                            },
                            "recentFailedMerges": [],
                            "mergeErrorsCount": 0,
                            "activeUsers24h": 3,
                        }
                    ),
                ]
            )
            db.commit()
        finally:
            db.close()

        with patch.dict(os.environ, {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY}, clear=False), patch(
            "app.main.start_scheduled_sync_service",
            new=self._async_noop,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=self._async_noop,
        ):
            with TestClient(app) as client:
                response = client.get(
                    "/api/client-health/history",
                    params={"days": 7},
                    headers=AUTH_HEADERS,
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [snapshot["snapshotDate"] for snapshot in response.json()["snapshots"]],
            ["2026-04-12"],
        )


class ErrorAnalysisEndpointTests(unittest.TestCase):
    @staticmethod
    async def _async_noop():
        return None

    def setUp(self):
        init_db()
        db = get_db()
        try:
            db.query(ErrorAnalysisGroup).filter(
                ErrorAnalysisGroup.school.in_(["bar01", "foo01", "bar02"])
            ).delete(synchronize_session=False)
            db.query(SchoolSnapshot).filter(
                SchoolSnapshot.school.in_(["bar01", "foo01", "bar02"])
            ).delete(synchronize_session=False)
            db.commit()
        finally:
            db.close()

    def tearDown(self):
        init_db()
        db = get_db()
        try:
            db.query(ErrorAnalysisGroup).filter(
                ErrorAnalysisGroup.school.in_(["bar01", "foo01", "bar02"])
            ).delete(synchronize_session=False)
            db.query(SchoolSnapshot).filter(
                SchoolSnapshot.school.in_(["bar01", "foo01", "bar02"])
            ).delete(synchronize_session=False)
            db.commit()
        finally:
            db.close()

    def test_error_analysis_returns_empty_state_before_capture(self):
        with patch.dict(os.environ, {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY}, clear=False), patch(
            "app.main.start_scheduled_sync_service",
            new=self._async_noop,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=self._async_noop,
        ):
            with TestClient(app) as client:
                response = client.get("/api/error-analysis", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["metadata"]["hasCapturedData"])
        self.assertEqual(payload["signatures"], [])

    def test_error_analysis_aggregates_filters_and_breakdowns(self):
        init_db()
        db = get_db()
        try:
            db.add_all(
                [
                    SchoolSnapshot.from_dict(
                        {
                            "snapshotDate": "2026-04-13",
                            "school": "bar01",
                            "displayName": "Baruch College",
                            "sisPlatform": "Banner",
                            "products": [],
                            "merges": {
                                "nightly": {"total": 1, "succeeded": 1, "failed": 0, "finishedWithIssues": 0, "noData": 0, "mergeTimeMs": 0},
                                "realtime": {"total": 0, "succeeded": 0, "failed": 0, "finishedWithIssues": 0, "noData": 0},
                                "manual": {"total": 0, "succeeded": 0, "failed": 0, "finishedWithIssues": 0, "noData": 0},
                            },
                            "recentFailedMerges": [],
                            "mergeErrorsCount": 3,
                            "activeUsers24h": 2,
                        }
                    ),
                    SchoolSnapshot.from_dict(
                        {
                            "snapshotDate": "2026-04-13",
                            "school": "foo01",
                            "displayName": "Foo State",
                            "sisPlatform": "PeopleSoftDirect",
                            "products": [],
                            "merges": {
                                "nightly": {"total": 1, "succeeded": 1, "failed": 0, "finishedWithIssues": 0, "noData": 0, "mergeTimeMs": 0},
                                "realtime": {"total": 0, "succeeded": 0, "failed": 0, "finishedWithIssues": 0, "noData": 0},
                                "manual": {"total": 0, "succeeded": 0, "failed": 0, "finishedWithIssues": 0, "noData": 0},
                            },
                            "recentFailedMerges": [],
                            "mergeErrorsCount": 2,
                            "activeUsers24h": 4,
                        }
                    ),
                    ErrorAnalysisGroup.from_dict(
                        {
                            "snapshotDate": "2026-04-12",
                            "school": "bar01",
                            "displayName": "Baruch College",
                            "sisPlatform": "Banner",
                            "entityType": "sections",
                            "errorCode": "missing_course",
                            "signatureKey": "sig-a",
                            "normalizedMessage": "course <num> missing dependency <num>",
                            "sampleMessage": "Course 202505 missing dependency 123456",
                            "count": 2,
                            "sampleErrors": [{"message": "Course 202505 missing dependency 123456", "lastSyncMergeReportId": "report-a1"}],
                            "termCodes": ["202505"],
                        }
                    ),
                    ErrorAnalysisGroup.from_dict(
                        {
                            "snapshotDate": "2026-04-13",
                            "school": "bar01",
                            "displayName": "Baruch College",
                            "sisPlatform": "Banner",
                            "entityType": "sections",
                            "errorCode": "missing_course",
                            "signatureKey": "sig-a",
                            "normalizedMessage": "course <num> missing dependency <num>",
                            "sampleMessage": "Course 202602 missing dependency 987654",
                            "count": 1,
                            "sampleErrors": [{"message": "Course 202602 missing dependency 987654", "lastSyncMergeReportId": "report-a2", "mergeReport": {"scheduleType": "nightly"}}],
                            "termCodes": ["202602"],
                        }
                    ),
                    ErrorAnalysisGroup.from_dict(
                        {
                            "snapshotDate": "2026-04-13",
                            "school": "foo01",
                            "displayName": "Foo State",
                            "sisPlatform": "PeopleSoftDirect",
                            "entityType": "courses",
                            "errorCode": "duplicate_course",
                            "signatureKey": "sig-b",
                            "normalizedMessage": "duplicate course <num>",
                            "sampleMessage": "Duplicate course 12345",
                            "count": 4,
                            "sampleErrors": [{"message": "Duplicate course 12345", "lastSyncMergeReportId": "report-b1", "mergeReport": {"scheduleType": "realtime"}}],
                            "termCodes": ["202505"],
                        }
                    ),
                    ErrorAnalysisGroup.from_dict(
                        {
                            "snapshotDate": "2026-04-14",
                            "school": "bar01",
                            "displayName": "Baruch College",
                            "sisPlatform": "Banner",
                            "entityType": "courses",
                            "errorCode": "duplicate_course",
                            "signatureKey": "sig-b",
                            "normalizedMessage": "duplicate course <num>",
                            "sampleMessage": "Duplicate course 99999",
                            "count": 1,
                            "sampleErrors": [{"message": "Duplicate course 99999", "lastSyncMergeReportId": "report-b2", "mergeReport": {"scheduleType": "nightly"}}],
                            "termCodes": ["202506"],
                        }
                    ),
                ]
            )
            db.commit()
        finally:
            db.close()

        with patch.dict(os.environ, {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY}, clear=False), patch(
            "app.main.start_scheduled_sync_service",
            new=self._async_noop,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=self._async_noop,
        ):
            with TestClient(app) as client:
                response = client.get("/api/error-analysis", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["metadata"]["hasCapturedData"])
        self.assertEqual(payload["summary"]["totalErrorInstances"], 8)
        self.assertEqual(payload["summary"]["distinctSignatures"], 2)
        self.assertEqual(payload["trends"][0]["snapshotDate"], "2026-04-12")
        self.assertEqual(payload["signatures"][0]["signatureKey"], "sig-b")
        self.assertEqual(payload["signatures"][0]["latestMergeReport"]["mergeReportId"], "report-b2")
        self.assertEqual(payload["signatures"][0]["dominantSchoolMergeReport"]["mergeReportId"], "report-b1")
        school_breakdowns = {row["key"]: row for row in payload["schoolBreakdowns"]}
        self.assertEqual(school_breakdowns["foo01"]["latestMergeReport"]["mergeReportId"], "report-b1")
        sis_breakdowns = {row["key"]: row for row in payload["sisBreakdowns"]}
        self.assertEqual(sis_breakdowns["PeopleSoftDirect"]["key"], "PeopleSoftDirect")
        self.assertEqual(len(payload["filterOptions"]["schools"]), 2)

    def test_error_analysis_filters_by_school_and_sis(self):
        init_db()
        db = get_db()
        try:
            db.add_all(
                [
                    ErrorAnalysisGroup.from_dict(
                        {
                            "snapshotDate": "2026-04-13",
                            "school": "bar01",
                            "displayName": "Baruch College",
                            "sisPlatform": "Banner",
                            "entityType": "sections",
                            "errorCode": "missing_course",
                            "signatureKey": "sig-a",
                            "normalizedMessage": "course missing dependency",
                            "sampleMessage": "Course missing dependency",
                            "count": 2,
                            "sampleErrors": [],
                            "termCodes": [],
                        }
                    ),
                    ErrorAnalysisGroup.from_dict(
                        {
                            "snapshotDate": "2026-04-13",
                            "school": "foo01",
                            "displayName": "Foo State",
                            "sisPlatform": "PeopleSoftDirect",
                            "entityType": "courses",
                            "errorCode": "duplicate_course",
                            "signatureKey": "sig-b",
                            "normalizedMessage": "duplicate course",
                            "sampleMessage": "Duplicate course",
                            "count": 4,
                            "sampleErrors": [],
                            "termCodes": [],
                        }
                    ),
                ]
            )
            db.commit()
        finally:
            db.close()

        with patch.dict(os.environ, {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY}, clear=False), patch(
            "app.main.start_scheduled_sync_service",
            new=self._async_noop,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=self._async_noop,
        ):
            with TestClient(app) as client:
                school_response = client.get(
                    "/api/error-analysis",
                    params={"school": "bar01"},
                    headers=AUTH_HEADERS,
                )
                sis_response = client.get(
                    "/api/error-analysis",
                    params={"sisPlatform": "PeopleSoftDirect"},
                    headers=AUTH_HEADERS,
                )

        self.assertEqual(school_response.status_code, 200)
        self.assertEqual(school_response.json()["summary"]["totalErrorInstances"], 2)
        self.assertEqual(school_response.json()["schoolBreakdowns"][0]["key"], "bar01")
        self.assertEqual(sis_response.status_code, 200)
        self.assertEqual(sis_response.json()["summary"]["totalErrorInstances"], 4)
        self.assertEqual(sis_response.json()["sisBreakdowns"][0]["key"], "PeopleSoftDirect")


class SyncRunListEndpointTests(unittest.TestCase):
    @staticmethod
    async def _async_noop():
        return None

    def setUp(self):
        init_db()
        db = get_db()
        try:
            db.query(SyncRun).filter(SyncRun.job_id.like("test-sync-run-%")).delete(synchronize_session=False)
            db.commit()
        finally:
            db.close()

    def test_sync_run_list_returns_pagination_metadata(self):
        init_db()
        db = get_db()
        try:
            db.add_all([
                SyncRun(
                    job_id="test-sync-run-1",
                    scope="history-backfill-bulk",
                    status="completed",
                    attempted_at=datetime(2026, 4, 12, 12, 0, tzinfo=timezone.utc),
                ),
                SyncRun(
                    job_id="test-sync-run-2",
                    scope="bulk",
                    status="running",
                    attempted_at=datetime(2026, 4, 12, 13, 0, tzinfo=timezone.utc),
                ),
                SyncRun(
                    job_id="test-sync-run-3",
                    scope="bulk",
                    status="failed",
                    attempted_at=datetime(2026, 4, 12, 14, 0, tzinfo=timezone.utc),
                ),
            ])
            db.commit()
        finally:
            db.close()

        with patch.dict(os.environ, {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY}, clear=False), patch(
            "app.main.start_scheduled_sync_service",
            new=self._async_noop,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=self._async_noop,
        ):
            with TestClient(app) as client:
                response = client.get("/api/client-health/sync-runs", params={"limit": 2, "offset": 1}, headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["totalCount"], 3)
        self.assertEqual(payload["limit"], 2)
        self.assertEqual(payload["offset"], 1)
        self.assertEqual([run["jobId"] for run in payload["syncRuns"]], ["test-sync-run-2", "test-sync-run-1"])

    def test_tests_use_isolated_database_path(self):
        self.assertEqual(get_database_url(), f"sqlite:///{TEST_DB_PATH}")
        self.assertNotEqual(TEST_DB_PATH, str(get_default_db_path()))


class SyncRunDetailEndpointTests(unittest.TestCase):
    @staticmethod
    async def _async_noop():
        return None

    def setUp(self):
        init_db()
        db = get_db()
        try:
            db.query(SyncRun).filter(SyncRun.job_id.like("job-detail-%")).delete(synchronize_session=False)
            db.commit()
        finally:
            db.close()

    def test_sync_run_detail_returns_one_persisted_run(self):
        with patch.dict(os.environ, {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY}, clear=False), patch(
            "app.main.start_scheduled_sync_service",
            new=self._async_noop,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=self._async_noop,
        ):
            init_db()
            db = get_db()
            try:
                db.add(
                    SyncRun(
                        job_id="job-detail-1",
                        scope="history-backfill-bulk",
                        status="completed",
                        attempted_at=datetime(2026, 4, 12, 12, 0, tzinfo=timezone.utc),
                    )
                )
                db.commit()
            finally:
                db.close()

            with TestClient(app) as client:
                response = client.get("/api/client-health/sync-runs/job-detail-1", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["syncRun"]["jobId"], "job-detail-1")

    def test_sync_run_detail_returns_404_for_missing_job(self):
        with patch.dict(os.environ, {"INTERNAL_API_KEY": TEST_INTERNAL_API_KEY}, clear=False), patch(
            "app.main.start_scheduled_sync_service",
            new=self._async_noop,
        ), patch(
            "app.main.stop_scheduled_sync_service",
            new=self._async_noop,
        ):
            with TestClient(app) as client:
                response = client.get("/api/client-health/sync-runs/missing-job", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
