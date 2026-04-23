"""Helpers for resolving verified school display names."""

from __future__ import annotations

from collections.abc import Mapping
from sqlalchemy.orm import Session

from app.models import BackfillWorkUnit, ErrorAnalysisDetail, ErrorAnalysisGroup, SchoolSnapshot

SCHOOL_NAME_MODELS: tuple[type, ...] = (
    SchoolSnapshot,
    ErrorAnalysisGroup,
    ErrorAnalysisDetail,
    BackfillWorkUnit,
)


def humanize_school_slug(school: str) -> str:
    """Turn a school slug into a readable fallback label."""
    normalized_school = school.strip()
    if not normalized_school:
        return ""
    parts = [part for part in normalized_school.replace("-", "_").split("_") if part]
    return " ".join(part[:1].upper() + part[1:] for part in parts)


def resolve_school_display_name(
    school: str,
    verified_display_name: str | None = None,
    fallback_display_name: str | None = None,
) -> str:
    """Return the best known human-readable name for a school."""
    normalized_verified = verified_display_name.strip() if verified_display_name else ""
    if normalized_verified:
        return normalized_verified

    normalized_fallback = fallback_display_name.strip() if fallback_display_name else ""
    if normalized_fallback:
        return normalized_fallback

    return humanize_school_slug(school)


def backfill_school_display_names(
    db: Session,
    verified_display_names: Mapping[str, str] | None = None,
) -> int:
    """Rewrite stored school display names to match the verified resolver."""
    updated = 0

    for model in SCHOOL_NAME_MODELS:
        rows = db.query(model).all()
        for row in rows:
            resolved = resolve_school_display_name(
                row.school,
                verified_display_name=(verified_display_names or {}).get(row.school),
                fallback_display_name=row.display_name,
            )
            if resolved != row.display_name:
                row.display_name = resolved
                updated += 1

    if updated:
        db.commit()

    return updated
