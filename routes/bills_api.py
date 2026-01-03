"""Bills API routes extracted from app.py to reduce file size.

This module intentionally keeps most logic as-is; later refactors can split it further.
"""

from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from flask import Blueprint, current_app, jsonify, request, send_file

from stores.project_store import stored_data

bills_bp = Blueprint("bills", __name__)

# =============================================================================
# BILL INTAKE ROUTES (Isolated from SiteWalk core - uses PostgreSQL)
# =============================================================================

# Resolved at runtime from app config (preferred) with env fallbacks.
BILLS_FEATURE_ENABLED = True
BILL_UPLOADS_DIR = "bill_uploads"


def _sync_bills_runtime_config() -> None:
    """
    Resolve config from `current_app.config` (set in app.py) with env fallbacks.
    Keep values in module-level globals to minimize refactor churn.
    """
    global BILLS_FEATURE_ENABLED, BILL_UPLOADS_DIR

    enabled = current_app.config.get("BILLS_FEATURE_ENABLED", None)
    if enabled is None:
        enabled = os.environ.get("UTILITY_BILLS_ENABLED", "true").strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
            "on",
        )
    BILLS_FEATURE_ENABLED = bool(enabled)

    uploads_dir = current_app.config.get("BILL_UPLOADS_DIR", None) or os.environ.get("BILL_UPLOADS_DIR")
    BILL_UPLOADS_DIR = str(uploads_dir or "bill_uploads")
    os.makedirs(BILL_UPLOADS_DIR, exist_ok=True)


_extraction_progress_lock = threading.Lock()
extraction_progress = {}

_bill_executor_lock = threading.Lock()
_bill_executor: ThreadPoolExecutor | None = None


def _get_bill_executor() -> ThreadPoolExecutor:
    global _bill_executor
    if _bill_executor is not None:
        return _bill_executor
    with _bill_executor_lock:
        if _bill_executor is not None:
            return _bill_executor
        try:
            max_workers = int(current_app.config.get("BILL_MAX_WORKERS", 3))
        except Exception:
            max_workers = 3
        _bill_executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="bill_processor")
        return _bill_executor

# Lazy initialization for bills database - prevents blocking during Gunicorn startup
_bills_db_initialized = False
_bills_db_init_lock = threading.Lock()

def ensure_bills_db_initialized():
    """Initialize bills database tables on first use (lazy loading).
    
    This prevents blocking during Gunicorn startup and ensures the app
    stays alive for health checks before DB is ready.
    """
    global _bills_db_initialized
    if _bills_db_initialized:
        return True
    
    with _bills_db_init_lock:
        if _bills_db_initialized:
            return True
        try:
            from bills_db import init_bills_tables
            init_bills_tables()
            _bills_db_initialized = True
            print("[bills] Database tables initialized (lazy)")
            return True
        except Exception as e:
            print(f"[bills] Warning: Could not initialize bills database: {e}")
            return False

# Import bills_db functions (but don't init tables yet)
try:
    from bills_db import (
        init_bills_tables, get_bill_files_for_project, add_bill_file, delete_bill_file, 
        get_meter_reads_for_project, get_bills_summary_for_project, update_bill_file_status,
        upsert_utility_account, upsert_utility_meter, upsert_meter_read, get_grouped_bills_data,
        update_bill_file_review_status, update_bill_file_extraction_payload, 
        get_files_status_for_project, get_bill_file_by_id,
        add_bill_screenshot, get_bill_screenshots, delete_bill_screenshot, 
        get_screenshot_count, mark_bill_ok,
        save_correction, get_corrections_for_utility, validate_extraction,
        get_bill_by_id, get_bill_review_data, update_bill, recompute_bill_file_missing_fields,
        find_bill_file_by_sha256
    )
    from bill_extractor import extract_bill_data, compute_missing_fields
    print("[bills] Bills module imported (tables will init on first request)")
except Exception as e:
    print(f"[bills] Warning: Could not import bills modules: {e}")


@bills_bp.before_app_request
def init_bills_db_on_demand():
    """Initialize bills database tables on first bills-related request.
    
    This is a lazy initialization pattern that ensures:
    1. App starts quickly for health checks
    2. Database init happens before any bills operation
    3. Init runs only once per worker
    """
    try:
        _sync_bills_runtime_config()
    except Exception:
        # Never break requests due to config sync; handlers will fall back to defaults.
        pass
    if request.path.startswith('/api/projects/') and '/bills' in request.path:
        ensure_bills_db_initialized()
    elif request.path.startswith('/api/bills'):
        ensure_bills_db_initialized()
    elif request.path.startswith('/api/accounts'):
        ensure_bills_db_initialized()


def populate_normalized_tables(project_id, extraction_result, source_filename, file_id=None):
    """
    Populate the normalized tables (utility_accounts, utility_meters, utility_meter_reads)
    from a successful extraction result.
    Also saves to new bills and bill_tou_periods tables.
    """
    try:
        # Also save to new normalized bills tables
        if file_id:
            try:
                from bill_extractor import save_bill_to_normalized_tables
                save_bill_to_normalized_tables(file_id, project_id, extraction_result)
            except Exception as bills_err:
                print(f"[bills] Warning: Error saving to new bills tables: {bills_err}")
        
        utility_name = extraction_result.get('utility_name')
        account_number = extraction_result.get('account_number')
        meters = extraction_result.get('meters', [])
        
        if not utility_name or not account_number:
            print(f"[bills] Cannot populate tables - missing utility_name or account_number")
            return False
        
        # Create/find account
        account_id = upsert_utility_account(project_id, utility_name, account_number)
        print(f"[bills] Upserted account: {utility_name} / {account_number} -> id={account_id}")
        
        total_reads = 0
        for meter in meters:
            meter_number = meter.get('meter_number')
            if not meter_number:
                continue
            
            # Create/find meter
            service_address = meter.get('service_address')
            meter_id = upsert_utility_meter(account_id, meter_number, service_address)
            print(f"[bills] Upserted meter: {meter_number} -> id={meter_id}")
            
            # Insert/update reads
            for read in meter.get('reads', []):
                period_start = read.get('period_start')
                period_end = read.get('period_end')
                kwh = read.get('kwh')
                total_charge = read.get('total_charge')
                
                if period_start and period_end:
                    upsert_meter_read(
                        meter_id=meter_id,
                        period_start=period_start,
                        period_end=period_end,
                        kwh=kwh,
                        total_charge=total_charge,
                        source_file=source_filename
                    )
                    total_reads += 1
        
        print(f"[bills] Populated {total_reads} reads for project {project_id}")
        return True
    except Exception as e:
        print(f"[bills] Error populating normalized tables: {e}")
        import traceback
        traceback.print_exc()
        return False


@bills_bp.route('/api/bills/enabled', methods=['GET'])
def bills_feature_status():
    """Check if bills feature is enabled."""
    return jsonify({'enabled': BILLS_FEATURE_ENABLED})


@bills_bp.route('/api/projects/<project_id>/bills', methods=['GET'])
def get_project_bills(project_id):
    """Get all bill data for a project."""
    if not BILLS_FEATURE_ENABLED:
        return jsonify({'error': 'Bills feature is disabled'}), 403
    
    try:
        files = get_bill_files_for_project(project_id)
        reads = get_meter_reads_for_project(project_id)
        summary = get_bills_summary_for_project(project_id)
        
        # Convert to serializable format
        files_list = []
        for f in files:
            files_list.append({
                'id': f['id'],
                'filename': f['filename'],
                'original_filename': f['original_filename'],
                'file_size': f['file_size'],
                'upload_date': f['upload_date'].isoformat() if f['upload_date'] else None,
                'processed': f['processed'],
                'processing_status': f['processing_status'],
                'review_status': f.get('review_status', 'pending')
            })
        
        reads_list = []
        for r in reads:
            kwh_val = float(r['kwh']) if r['kwh'] else None
            total_charges_val = float(r['total_charges_usd']) if r['total_charges_usd'] else None
            reads_list.append({
                'id': r['id'],
                'utility_name': r['utility_name'],
                'account_number': r['account_number'],
                'meter_number': r['meter_number'],
                'billing_start_date': r['billing_start_date'].isoformat() if r['billing_start_date'] else None,
                'billing_end_date': r['billing_end_date'].isoformat() if r['billing_end_date'] else None,
                'statement_date': r['statement_date'].isoformat() if r['statement_date'] else None,
                # Legacy keys (kept for backward compatibility)
                'kwh': kwh_val,
                'total_charges_usd': total_charges_val,
                # Canonical keys (PR4): align with bills table naming
                'total_kwh': kwh_val,
                'total_amount_due': total_charges_val,
                'source_file': r['source_file'],
                'source_page': r['source_page'],
                'from_summary_table': r['from_summary_table']
            })
        
        return jsonify({
            'success': True,
            'project_id': project_id,
            'files': files_list,
            'meter_reads': reads_list,
            'summary': {
                'file_count': summary['file_count'] if summary else 0,
                'account_count': summary['account_count'] if summary else 0,
                'read_count': summary['read_count'] if summary else 0
            }
        })
    except Exception as e:
        print(f"[bills] Error getting bills for project {project_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@bills_bp.route('/api/projects/<project_id>/bills/upload', methods=['POST'])
def upload_bill_file(project_id):
    """Upload a bill PDF file for a project. Does NOT trigger extraction - use /process endpoint."""
    import hashlib
    from werkzeug.utils import secure_filename
    from bill_intake.db.connection import get_connection
    
    if not BILLS_FEATURE_ENABLED:
        return jsonify({'error': 'Bills feature is disabled'}), 403

    # Ensure the bills DB is configured/initialized; otherwise return a clear, actionable error.
    # Without DATABASE_URL, downstream DB calls will raise and the UI will look like uploads "reset".
    try:
        # Validate DB connectivity early so the frontend gets a clean error instead of a silent reset.
        conn = get_connection()
        conn.close()

        if not ensure_bills_db_initialized():
            return jsonify({
                'success': False,
                'error': 'Bills database could not be initialized. Check DATABASE_URL and database connectivity.'
            }), 503
    except Exception:
        return jsonify({
            'success': False,
            'error': 'Bills database is not configured/reachable. Set DATABASE_URL (PostgreSQL) and ensure the database is running.'
        }), 503
    
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'}), 400
    
    # Validate file type - accept PDFs and images
    allowed_extensions = {'pdf', 'jpg', 'jpeg', 'png', 'heic', 'webp', 'gif'}
    if '.' not in file.filename or file.filename.rsplit('.', 1)[1].lower() not in allowed_extensions:
        return jsonify({'success': False, 'error': 'Allowed file types: PDF, JPG, PNG, HEIC, WEBP, GIF'}), 400
    
    try:
        # Read file content and compute SHA-256 hash
        file_content = file.read()
        file_sha256 = hashlib.sha256(file_content).hexdigest()
        file.seek(0)  # Reset file pointer for saving
        
        # Check for duplicate by SHA-256
        existing = find_bill_file_by_sha256(project_id, file_sha256)
        if existing:
            print(f"[bills] Duplicate file detected: sha256={file_sha256[:12]}... matches file_id={existing['id']}")
            return jsonify({
                'success': True,
                'is_duplicate': True,
                'file': {
                    'id': existing['id'],
                    'filename': existing['filename'],
                    'original_filename': existing['original_filename'],
                    'file_size': existing['file_size'],
                    'upload_date': existing['upload_date'].isoformat() if existing['upload_date'] else None,
                    'review_status': existing['review_status'],
                    'processing_status': existing['processing_status'],
                    'sha256': existing['sha256'],
                    'service_type': existing.get('service_type', 'electric')
                }
            }), 200
        
        # Generate unique filename
        original_filename = secure_filename(file.filename)
        if not original_filename:
            return jsonify({'success': False, 'error': 'Invalid filename'}), 400
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_filename = f"{project_id}_{timestamp}_{original_filename}"
        file_path = os.path.join(BILL_UPLOADS_DIR, unique_filename)
        
        # Save file
        with open(file_path, 'wb') as f:
            f.write(file_content)
        file_size = os.path.getsize(file_path)
        
        # Add record to database with status = 'pending' (no processing yet)
        record = add_bill_file(
            project_id=project_id,
            filename=unique_filename,
            original_filename=original_filename,
            file_path=file_path,
            file_size=file_size,
            mime_type=file.content_type or 'application/octet-stream',
            sha256=file_sha256
        )
        
        print(f"[bills] Uploaded file: {unique_filename} for project {project_id}, file_id={record['id']}, sha256={file_sha256[:12]}...")
        
        # Return immediately with file ID - caller must use /process endpoint for extraction
        return jsonify({
            'success': True,
            'is_duplicate': False,
            'file': {
                'id': record['id'],
                'filename': record['filename'],
                'original_filename': record['original_filename'],
                'file_size': record['file_size'],
                'upload_date': record['upload_date'].isoformat() if record['upload_date'] else None,
                'review_status': record['review_status'],
                'sha256': record.get('sha256'),
                'service_type': record.get('service_type', 'electric')
            }
        }), 201
        
    except Exception as e:
        # Make missing DB configuration an actionable (503) error instead of a generic 500.
        if "DATABASE_URL not configured" in str(e):
            return jsonify({
                'success': False,
                'error': 'Bills database is not configured. Set DATABASE_URL (PostgreSQL) to enable utility bills.'
            }), 503
        print(f"[bills] Error uploading file: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


def _run_bill_extraction(project_id, file_id, file_path, original_filename):
    """Background worker function to run bill extraction in thread pool."""
    import time
    
    try:
        # Progress callback that updates extraction_progress
        def progress_callback(progress_value, status_message=None):
            extraction_progress[file_id] = {
                'status': 'extracting',
                'progress': progress_value,
                'message': status_message,
                'updated_at': time.time(),
                'project_id': project_id
            }
        
        print(f"[bills] Background processing file: {original_filename} (id={file_id})")
        
        # First pass extraction without hints to detect utility
        extraction_result = extract_bill_data(file_path, progress_callback=progress_callback)
        
        # If first pass got a utility name, look up training hints and re-extract
        utility_name = extraction_result.get('utility_name')
        if utility_name:
            try:
                training_hints = get_corrections_for_utility(utility_name)
                if training_hints and len(training_hints) > 0:
                    print(f"[bills] Found {len(training_hints)} training hints for {utility_name}, re-extracting...")
                    extraction_result = extract_bill_data(file_path, progress_callback=progress_callback, training_hints=training_hints)
            except Exception as hint_err:
                print(f"[bills] Warning: Could not get training hints: {hint_err}")
        
        # Store raw extraction result in extraction_payload
        update_bill_file_extraction_payload(file_id, extraction_result)
        
        if extraction_result.get('success'):
            # Compute missing fields for tracking
            missing_fields = compute_missing_fields(extraction_result)
            
            # Run validation to determine if 'ok' or 'needs_review'
            validation = validate_extraction(extraction_result)
            
            if validation['is_valid'] and len(missing_fields) == 0:
                review_status = 'ok'
                print(f"[bills] Extraction valid - status 'ok'")
            else:
                review_status = 'needs_review'
                all_missing = list(set(validation.get('missing_fields', []) + missing_fields))
                print(f"[bills] Extraction needs review: {all_missing[:3]}...")
            
            update_bill_file_review_status(file_id, review_status)
            update_bill_file_status(file_id, 'extracted', processed=True, missing_fields=missing_fields)
            
            # CRITICAL: Populate normalized tables so Extracted Data section shows data
            populate_normalized_tables(project_id, extraction_result, original_filename, file_id=file_id)
            
            # Update progress to final status
            extraction_progress[file_id] = {
                'status': review_status,
                'progress': 1.0,
                'updated_at': time.time(),
                'project_id': project_id
            }
            
            meters_count = len(extraction_result.get('meters', []))
            reads_count = sum(len(m.get('reads', [])) for m in extraction_result.get('meters', []))
            print(f"[bills] Extraction complete: {meters_count} meters, {reads_count} reads - status: {review_status}")
        else:
            # Extraction failed - mark as error
            error_msg = extraction_result.get('error', 'Unknown extraction error')
            update_bill_file_review_status(file_id, 'error')
            update_bill_file_status(file_id, 'error', processed=True)
            
            # Update progress to error status
            extraction_progress[file_id] = {
                'status': 'needs_review',
                'progress': 1.0,
                'updated_at': time.time(),
                'project_id': project_id
            }
            
            print(f"[bills] Extraction failed: {error_msg}")
        
    except Exception as e:
        print(f"[bills] Background processing error for file {file_id}: {e}")
        import traceback
        traceback.print_exc()
        update_bill_file_review_status(file_id, 'error')
        extraction_progress[file_id] = {
            'status': 'error',
            'progress': 1.0,
            'updated_at': time.time(),
            'project_id': project_id
        }


@bills_bp.route('/api/projects/<project_id>/bills/process/<int:file_id>', methods=['POST'])
def process_bill_file(project_id, file_id):
    """Trigger extraction for a single bill file. Returns immediately, runs in background."""
    import time
    
    if not BILLS_FEATURE_ENABLED:
        return jsonify({'error': 'Bills feature is disabled'}), 403
    
    try:
        # Clean up old progress entries periodically
        cleanup_old_progress_entries()
        
        # Get file record - validate before any state changes
        file_record = get_bill_file_by_id(file_id)
        if not file_record:
            return jsonify({'success': False, 'error': 'File not found'}), 404
        
        if file_record['project_id'] != project_id:
            return jsonify({'success': False, 'error': 'File does not belong to this project'}), 403
        
        # DUPLICATE PREVENTION: Check if already processing or completed
        current_review_status = file_record.get('review_status', 'pending')
        current_proc_status = file_record.get('processing_status', 'pending')
        
        # Check if in progress tracker with extracting status
        if file_id in extraction_progress:
            prog_status = extraction_progress[file_id].get('status')
            if prog_status == 'extracting':
                print(f"[bills] Duplicate extraction request ignored - file {file_id} is already extracting")
                return jsonify({
                    'success': True,
                    'file_id': file_id,
                    'status': 'already_processing',
                    'message': 'File is already being processed'
                })
        
        # Check if already processing or completed
        if current_review_status == 'processing':
            print(f"[bills] Duplicate extraction request ignored - file {file_id} has processing status")
            return jsonify({
                'success': True,
                'file_id': file_id,
                'status': 'already_processing',
                'message': 'File is already being processed'
            })
        
        # Skip if already successfully processed
        if current_review_status in ('ok', 'needs_review') and file_record.get('processed'):
            print(f"[bills] Duplicate extraction request ignored - file {file_id} is already processed")
            return jsonify({
                'success': True,
                'file_id': file_id,
                'status': 'already_complete',
                'message': 'File has already been processed'
            })
        
        file_path = file_record['file_path']
        original_filename = file_record['original_filename']
        
        # Validate file exists before queuing
        if not os.path.exists(file_path):
            return jsonify({'success': False, 'error': 'File not found on disk'}), 404
        
        # Set status to processing first
        update_bill_file_review_status(file_id, 'processing')
        
        # Check extraction method - default to 'text' (new pipeline), 'vision' for legacy
        extraction_method = request.args.get('method', 'text')
        use_text_extraction = extraction_method == 'text'
        
        if use_text_extraction:
            # Use new text-based extraction with JobQueue
            from bills.job_queue import get_job_queue
            from bill_extractor import extract_bill_data_text_based
            
            job_queue = get_job_queue()
            
            # Check if already in job queue
            if job_queue.is_processing(file_id):
                print(f"[bills] File {file_id} already in job queue")
                return jsonify({
                    'success': True,
                    'file_id': file_id,
                    'status': 'already_processing',
                    'message': 'File is already being processed'
                })
            
            # Define completion callback
            def on_extraction_complete(fid, result):
                if result.get('success', True):
                    update_bill_file_review_status(fid, 'ok' if result.get('confidence', 0) > 0.7 else 'needs_review')
                else:
                    update_bill_file_review_status(fid, 'error')
            
            # Submit to JobQueue
            submitted = job_queue.submit(
                file_id,
                extract_bill_data_text_based,
                file_path,
                project_id,
                on_complete=on_extraction_complete
            )
            
            if not submitted:
                return jsonify({
                    'success': True,
                    'file_id': file_id,
                    'status': 'already_processing',
                    'message': 'File is already being processed'
                })
            
            print(f"[bills] Queued file for text-based extraction: {original_filename} (id={file_id})")
            
            return jsonify({
                'success': True,
                'file_id': file_id,
                'status': 'processing',
                'method': 'text',
                'message': 'Text-based extraction started in background'
            })
        else:
            # Legacy vision-based extraction
            extraction_progress[file_id] = {
                'status': 'extracting',
                'progress': 0.0,
                'updated_at': time.time(),
                'project_id': project_id
            }
            
            try:
                future = _get_bill_executor().submit(_run_bill_extraction, project_id, file_id, file_path, original_filename)
                print(f"[bills] Queued file for vision-based processing: {original_filename} (id={file_id})")
            except Exception as submit_err:
                print(f"[bills] Failed to queue file {file_id}: {submit_err}")
                extraction_progress[file_id] = {
                    'status': 'error',
                    'progress': 1.0,
                    'updated_at': time.time(),
                    'project_id': project_id
                }
                update_bill_file_review_status(file_id, 'error')
                return jsonify({'success': False, 'error': f'Failed to start processing: {submit_err}'}), 500
            
            return jsonify({
                'success': True,
                'file_id': file_id,
                'status': 'processing',
                'method': 'vision',
                'message': 'Vision-based extraction started in background'
            })
        
    except Exception as e:
        print(f"[bills] Error in process_bill_file: {e}")
        import traceback
        traceback.print_exc()
        # Clean up progress state on error
        extraction_progress[file_id] = {
            'status': 'error',
            'progress': 1.0,
            'updated_at': time.time(),
            'project_id': project_id
        }
        try:
            update_bill_file_review_status(file_id, 'error')
        except:
            pass
        return jsonify({'success': False, 'error': str(e)}), 500


@bills_bp.route('/api/bills/file/<int:file_id>/progress', methods=['GET'])
def get_bill_file_progress(file_id):
    """Get extraction progress for a single file."""
    if not BILLS_FEATURE_ENABLED:
        return jsonify({'error': 'Bills feature is disabled'}), 403
    
    # Check if we have progress info for this file
    if file_id in extraction_progress:
        progress_data = extraction_progress[file_id]
        return jsonify({
            'status': progress_data.get('status', 'pending'),
            'progress': progress_data.get('progress', 0.0)
        })
    
    # If not in progress tracker, check the file's actual status
    try:
        file_record = get_bill_file_by_id(file_id)
        if not file_record:
            return jsonify({'status': 'pending', 'progress': 0.0})
        
        review_status = file_record.get('review_status', 'pending')
        
        # Map review_status to progress response
        if review_status in ('ok', 'needs_review'):
            return jsonify({'status': review_status, 'progress': 1.0})
        elif review_status == 'processing':
            return jsonify({'status': 'extracting', 'progress': 0.0})
        else:
            return jsonify({'status': 'pending', 'progress': 0.0})
    except Exception as e:
        print(f"[bills] Error getting progress: {e}")
        return jsonify({'status': 'pending', 'progress': 0.0})


@bills_bp.route('/api/bills/status/<int:file_id>', methods=['GET'])
def get_bill_processing_status(file_id):
    """Get granular processing status for a bill file using JobQueue."""
    if not BILLS_FEATURE_ENABLED:
        return jsonify({'error': 'Bills feature is disabled'}), 403
    
    from bills.job_queue import get_job_queue
    job_queue = get_job_queue()
    status = job_queue.get_status_dict(file_id)
    
    if status:
        return jsonify({'success': True, **status})
    
    file_record = get_bill_file_by_id(file_id)
    if file_record:
        return jsonify({
            'success': True,
            'file_id': file_id,
            'state': file_record.get('processing_status', 'unknown'),
            'progress': 1.0 if file_record.get('processed') else 0.0
        })
    
    return jsonify({'success': False, 'error': 'File not found'}), 404


@bills_bp.route('/api/projects/<project_id>/bills/status', methods=['GET'])
def get_bills_status(project_id):
    """Get status of all bill files for a project (for polling)."""
    if not BILLS_FEATURE_ENABLED:
        return jsonify({'error': 'Bills feature is disabled'}), 403
    
    try:
        files = get_files_status_for_project(project_id)
        
        # Count queue depth from extraction_progress - only for this project
        queue_depth = sum(1 for fid, prog in extraction_progress.items() 
                        if prog.get('status') == 'extracting' and prog.get('project_id') == project_id)
        
        files_list = []
        for f in files:
            file_id = f['id']
            # Add queue position for extracting files
            queue_position = None
            if file_id in extraction_progress and extraction_progress[file_id].get('status') == 'extracting':
                # Estimate position based on order in dict (not perfect but gives idea)
                extracting_ids = [fid for fid, prog in extraction_progress.items() 
                                 if prog.get('status') == 'extracting']
                if file_id in extracting_ids:
                    queue_position = extracting_ids.index(file_id) + 1
            
            files_list.append({
                'id': file_id,
                'original_filename': f['original_filename'],
                'review_status': f['review_status'],
                'processing_status': f['processing_status'],
                'processed': f['processed'],
                'upload_date': f['upload_date'].isoformat() if f['upload_date'] else None,
                'queue_position': queue_position
            })
        
        return jsonify({
            'success': True,
            'project_id': project_id,
            'files': files_list,
            'queue_depth': queue_depth,
            'max_workers': 3
        })
    except Exception as e:
        print(f"[bills] Error getting status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@bills_bp.route('/api/projects/<project_id>/bills/job-status', methods=['GET'])
def get_bills_job_status(project_id):
    """Get aggregated job status for all bill files in a project (for progress bar polling)."""
    if not BILLS_FEATURE_ENABLED:
        return jsonify({'error': 'Bills feature is disabled'}), 403
    
    try:
        files = get_files_status_for_project(project_id)
        
        total = len(files)
        complete = 0
        in_progress = 0
        failed = 0
        needs_review = 0
        
        for f in files:
            review_status = f.get('review_status', 'pending')
            processing_status = f.get('processing_status', 'pending')
            
            if review_status == 'ok':
                complete += 1
            elif review_status == 'needs_review':
                needs_review += 1
            elif processing_status == 'error' or review_status == 'error':
                failed += 1
            else:
                in_progress += 1
        
        return jsonify({
            'total': total,
            'complete': complete,
            'inProgress': in_progress,
            'failed': failed,
            'needsReview': needs_review
        })
    except Exception as e:
        print(f"[bills] Error getting job status: {e}")
        return jsonify({'error': str(e)}), 500
def _is_enabled() -> bool:
    return bool(BILLS_FEATURE_ENABLED)


# Register the remaining bills endpoints from smaller modules.
try:
    from routes.bills_api_part2 import register as _register_part2
    from routes.bills_api_part3 import register as _register_part3

    _register_part2(bills_bp=bills_bp, is_enabled=_is_enabled, populate_normalized_tables=populate_normalized_tables)
    _register_part3(
        bills_bp=bills_bp,
        is_enabled=_is_enabled,
        extraction_progress=extraction_progress,
        populate_normalized_tables=populate_normalized_tables,
    )
except Exception as e:
    print(f"[bills] Warning: could not register all bills routes: {e}")

