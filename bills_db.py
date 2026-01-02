"""
Bill Intake Database Module (compatibility facade)

This file intentionally stays *small* and re-exports the public DB API from the
newly modularized `bill_intake` package. Existing imports like:

    from bills_db import get_connection, init_bills_tables

continue to work unchanged.
"""

from __future__ import annotations

# Connection / common normalization
from bill_intake.db.connection import DATABASE_URL, get_connection
from bill_intake.utils.normalization import (
    normalize_account_number,
    normalize_meter_number,
    normalize_utility_name,
)

# Schema / migrations
from bill_intake.db.schema import init_bills_tables

# File-level operations + caching
from bill_intake.db.bill_files import (
    add_bill_file,
    delete_bill_file,
    find_bill_file_by_sha256,
    get_bill_file_by_id,
    get_bill_files_for_project,
    get_cached_result_by_hash,
    get_files_status_for_project,
    invalidate_cache_for_file,
    mark_bill_ok,
    save_cache_entry,
    update_bill_file_extraction_payload,
    update_bill_file_review_status,
    update_bill_file_status,
    update_file_processing_status,
)

# Accounts / meters / reads
from bill_intake.db.accounts import get_utility_accounts_for_project, upsert_utility_account
from bill_intake.db.meters import upsert_utility_meter
from bill_intake.db.meter_reads import get_meter_reads_for_project, upsert_meter_read

# Bills (normalized) write + read + update
from bill_intake.db.bills_write import delete_bills_for_file, insert_bill, insert_bill_tou_period
from bill_intake.db.bills_read import (
    get_account_summary,
    get_bill_by_id,
    get_bill_detail,
    get_bill_review_data,
    get_bills_summary_for_project,
    get_grouped_bills_data,
    get_meter_bills,
    get_meter_months,
)
from bill_intake.db.bills_update import recompute_bill_file_missing_fields, update_bill

# Screenshots + training
from bill_intake.db.screenshots import (
    add_bill_screenshot,
    delete_bill_screenshot,
    get_bill_screenshots,
    get_screenshot_count,
)
from bill_intake.db.training import get_corrections_for_utility, save_correction

# Maintenance + cloning + export
from bill_intake.db.maintenance import delete_account_if_empty, delete_all_empty_accounts
from bill_intake.db.clone import clone_bills_for_project
from bill_intake.db.export import export_bills_csv

# Validation helpers
from bill_intake.validation import validate_extraction


__all__ = [
    # Connection / normalization
    "DATABASE_URL",
    "get_connection",
    "normalize_account_number",
    "normalize_meter_number",
    "normalize_utility_name",
    # Schema
    "init_bills_tables",
    # Files / cache
    "add_bill_file",
    "delete_bill_file",
    "find_bill_file_by_sha256",
    "get_bill_file_by_id",
    "get_bill_files_for_project",
    "get_cached_result_by_hash",
    "get_files_status_for_project",
    "invalidate_cache_for_file",
    "mark_bill_ok",
    "save_cache_entry",
    "update_bill_file_extraction_payload",
    "update_bill_file_review_status",
    "update_bill_file_status",
    "update_file_processing_status",
    # Accounts / meters / reads
    "get_utility_accounts_for_project",
    "upsert_utility_account",
    "upsert_utility_meter",
    "get_meter_reads_for_project",
    "upsert_meter_read",
    # Bills
    "delete_bills_for_file",
    "insert_bill",
    "insert_bill_tou_period",
    "get_account_summary",
    "get_bill_by_id",
    "get_bill_detail",
    "get_bill_review_data",
    "get_bills_summary_for_project",
    "get_grouped_bills_data",
    "get_meter_bills",
    "get_meter_months",
    "update_bill",
    "recompute_bill_file_missing_fields",
    # Screenshots + training
    "add_bill_screenshot",
    "get_bill_screenshots",
    "delete_bill_screenshot",
    "get_screenshot_count",
    "save_correction",
    "get_corrections_for_utility",
    # Maintenance + cloning + export
    "delete_account_if_empty",
    "delete_all_empty_accounts",
    "clone_bills_for_project",
    "export_bills_csv",
    # Validation
    "validate_extraction",
]
