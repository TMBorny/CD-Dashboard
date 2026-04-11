import unittest

from fastapi.testclient import TestClient

from app.main import app
from app.routes import (
    EXCLUDED_SCHOOL_TERMS,
    build_snapshot_from_merge_history,
    extract_unique_activity_users,
    extract_merge_history_entries,
    extract_merge_error_count,
    extract_sis_platform,
    is_demo_school,
    select_schools_for_sync,
    serialize_sync_job,
    summarize_recent_merge_activity,
)


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
        with TestClient(app) as client:
            response = client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})


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
        self.assertEqual(snapshot["merges"]["realtime"]["total"], 1)
        self.assertEqual(snapshot["merges"]["manual"]["failed"], 1)
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
            (2, 1, 1, 1),
        )

    def test_extract_sis_platform_prefers_platform_name(self):
        payload = {"sisPlatform": "Banner", "integrationBroker": "Ethos"}
        self.assertEqual(extract_sis_platform(payload), "Banner")


class SchoolSelectionTests(unittest.TestCase):
    def test_bulk_sync_is_temporarily_limited(self):
        schools = [{"school": f"school-{index}"} for index in range(12)]
        selected = select_schools_for_sync(schools, school=None)
        self.assertEqual(len(selected), 10)
        self.assertEqual(selected[0]["school"], "school-0")

    def test_single_school_sync_returns_requested_school(self):
        schools = [{"school": "bar01"}, {"school": "abc01"}]
        selected = select_schools_for_sync(schools, school="bar01")
        self.assertEqual(selected, [{"school": "bar01"}])


class DemoSchoolTests(unittest.TestCase):
    def test_excluded_terms_are_centralized(self):
        self.assertEqual(EXCLUDED_SCHOOL_TERMS, ("demo", "test", "sandbox", "baseline"))

    def test_detects_demo_school_by_school_id(self):
        self.assertTrue(is_demo_school("demo_baruch", "Baruch Demo"))

    def test_detects_demo_school_by_display_name(self):
        self.assertTrue(is_demo_school("bar01", "Baruch Demo Tenant"))

    def test_non_demo_school_is_not_filtered(self):
        self.assertFalse(is_demo_school("bar01", "Baruch College"))


if __name__ == "__main__":
    unittest.main()
