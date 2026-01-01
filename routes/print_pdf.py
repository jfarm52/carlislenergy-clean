from __future__ import annotations

import logging
from datetime import datetime

from flask import Blueprint, jsonify, send_file

from stores.project_store import stored_data

logger = logging.getLogger(__name__)

print_pdf_bp = Blueprint("print_pdf", __name__)


@print_pdf_bp.get("/api/projects/<project_id>/print.pdf")
def get_project_print_pdf(project_id: str):
    """Generate PDF from print view HTML using weasyprint."""
    try:
        from weasyprint import HTML
        import io

        # Get project data
        user_id = "default"
        if user_id not in stored_data or project_id not in stored_data[user_id]:
            return jsonify({"error": "Project not found"}), 404

        project = stored_data[user_id][project_id]
        sd = project.get("siteData", {})
        entries = project.get("entries", [])

        customer = sd.get("customer", "Project")
        visit_date = sd.get("visitDate", sd.get("date", ""))

        # Build print HTML (same structure as frontend)
        evaps = [e for e in entries if e.get("section") == "evap"]
        conds = [e for e in entries if e.get("section") == "cond"]

        def spec_pair(label, value):
            return f'<span class="spec-label">{label}:</span> <span class="spec-value">{value}</span>'

        def build_mfr_html(entry):
            if not entry.get("room-mfg"):
                return ""
            return f'<div class="print-room-mfr"><span class="mfr-label">Mfr:</span> <span class="mfr-value">{entry.get("room-mfg")}</span></div>'

        def build_info_line(entry, is_evap):
            parts = []
            if is_evap:
                if entry.get("room-setPoint"):
                    parts.append(
                        f'<span class="mfr-label">Set Point:</span> <span class="mfr-value">{entry.get("room-setPoint")}°F</span>'
                    )
                if entry.get("room-currentTemp"):
                    parts.append(
                        f'<span class="mfr-label">Current Temp:</span> <span class="mfr-value">{entry.get("room-currentTemp")}°F</span>'
                    )
            if entry.get("room-runTime"):
                parts.append(
                    f'<span class="mfr-label">Run Time:</span> <span class="mfr-value">{entry.get("room-runTime")}%</span>'
                )
            if not is_evap and entry.get("room-split") is True:
                parts.append("Split")
            return f'<div class="print-room-info">{" | ".join(parts)}</div>' if parts else ""

        def build_spec_columns(entry, is_evap):
            left_lines = []
            right_lines = []

            # Left column
            line1 = []
            if entry.get("room-count"):
                line1.append(spec_pair("Units", entry.get("room-count")))
            if entry.get("room-fanMotorsPerUnit"):
                line1.append(spec_pair("Motors Per Unit", entry.get("room-fanMotorsPerUnit")))
            if line1:
                left_lines.append(" | ".join(line1))

            line2 = []
            if entry.get("room-voltage"):
                line2.append(spec_pair("Voltage", entry.get("room-voltage")))
            if entry.get("room-phase"):
                line2.append(spec_pair("Phase", entry.get("room-phase")))
            if entry.get("room-amps"):
                line2.append(spec_pair("FLA", entry.get("room-amps")))
            if entry.get("room-hp"):
                line2.append(spec_pair("HP", entry.get("room-hp")))
            if entry.get("room-rpm"):
                line2.append(spec_pair("RPM", entry.get("room-rpm")))
            if line2:
                left_lines.append(" | ".join(line2))

            # Right column
            r_line1 = []
            if entry.get("room-frame"):
                r_line1.append(spec_pair("Frame", entry.get("room-frame")))
            if entry.get("room-motorMounting"):
                mount_val = (entry.get("room-motorMounting", "") or "").capitalize()
                r_line1.append(spec_pair("Mount", mount_val))
            if entry.get("room-shaftSize"):
                r_line1.append(spec_pair("Shaft", entry.get("room-shaftSize")))
            if entry.get("room-rotation"):
                r_line1.append(spec_pair("Rotation", entry.get("room-rotation")))
            if r_line1:
                right_lines.append(" | ".join(r_line1))

            r_line2 = []
            if entry.get("room-shaftAdapterQty") and int(entry.get("room-shaftAdapterQty", 0)) > 0 and entry.get(
                "room-shaftAdapterType"
            ):
                r_line2.append(
                    f'<span class="spec-label">Adapters:</span> <span class="spec-value">({entry.get("room-shaftAdapterQty")}) {entry.get("room-shaftAdapterType")}</span>'
                )
            if entry.get("room-bladesNeeded") and int(entry.get("room-bladesNeeded", 0)) > 0 and entry.get(
                "room-bladeSpec"
            ):
                r_line2.append(
                    f'<span class="spec-label">FanBlade(s):</span> <span class="spec-value">({entry.get("room-bladesNeeded")}) {entry.get("room-bladeSpec")}</span>'
                )
            elif entry.get("room-bladeSpec"):
                r_line2.append(
                    f'<span class="spec-label">FanBlade(s):</span> <span class="spec-value">{entry.get("room-bladeSpec")}</span>'
                )
            if r_line2:
                right_lines.append(" | ".join(r_line2))

            if not left_lines and not right_lines:
                return ""

            left_html = "".join([f'<div class="spec-line">{l}</div>' for l in left_lines])
            right_html = "".join([f'<div class="spec-line">{l}</div>' for l in right_lines])

            if not right_lines:
                return f'<div class="print-room-specs"><div class="spec-column spec-left">{left_html}</div></div>'

            return f'<div class="print-room-specs"><div class="spec-column spec-left">{left_html}</div><div class="spec-column spec-right">{right_html}</div></div>'

        html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Print View - {customer}</title>
  <style>
    @page {{ size: letter; margin: 0.5in; }}
    body {{ font-family: Arial, sans-serif; margin: 16px; color: #333; }}
    .print-project {{ max-width: 1100px; margin: 0 auto; padding: 10px 16px; }}
    h1 {{ color: #1e5a99; margin-bottom: 0.5rem; }}
    h2 {{ color: #2d7bb8; border-bottom: 2px solid #2d7bb8; padding-bottom: 0.25rem; margin-top: 1.5rem; }}
    .site-info {{ background: #f5f5f5; padding: 0.75rem 1rem; border-radius: 4px; margin-bottom: 1.5rem; }}
    .site-info p {{ margin: 0.2rem 0; font-size: 0.95rem; }}
    .print-room {{ border: 1px solid #bbb; border-radius: 4px; padding: 10px 14px; margin-bottom: 12px; page-break-inside: avoid; }}
    .room-title {{ font-weight: 700; font-size: 1.15rem; color: #1e5a99; margin-bottom: 4px; }}
    .print-room-mfr {{ font-size: 0.9rem; margin-bottom: 6px; }}
    .mfr-label {{ font-weight: 400; color: #777; }}
    .mfr-value {{ font-weight: 400; color: #000; }}
    .spec-label {{ font-weight: 700; color: #000; }}
    .spec-value {{ font-weight: 400; color: #000; }}
    .print-room-info {{ font-size: 0.85rem; color: #555; margin-bottom: 6px; }}
    .print-room-specs {{ display: flex; justify-content: space-between; gap: 40px; margin-bottom: 6px; }}
    .spec-column {{ flex: 1; text-align: left; }}
    .spec-line {{ margin-bottom: 3px; font-size: 0.95rem; color: #333; white-space: normal; line-height: 1.4; }}
    .print-notes-separator {{ border: 0; border-top: 1px solid #dddddd; margin: 6px 0 4px 0; }}
    .print-room-notes {{ font-size: 0.9rem; }}
    .print-room-notes .notes-label {{ font-weight: 600; }}
    .print-room-notes .notes-text {{ font-weight: normal; white-space: pre-wrap; color: #555; }}
  </style>
</head>
<body>
<div class="print-project">
  <h1>{customer}</h1>
  <div class="site-info">
    <p><strong>Address:</strong> {sd.get('street', '')}, {sd.get('city', '')}, {sd.get('state', '')} {sd.get('zip', '')}</p>
    <p><strong>Contact:</strong> {sd.get('contact', '')} {('(' + sd.get('phone') + ')') if sd.get('phone') else ''}</p>
    <p><strong>Utility:</strong> {sd.get('utility', '')}</p>
    <p><strong>Date of Site Visit:</strong> {visit_date}</p>
  </div>
"""

        if evaps:
            html += "<h2>Evaporators</h2>"
            for i, e in enumerate(evaps):
                notes = e.get("room-notes", "") or "—"
                room_name = e.get("room-name", f"Evaporator {i + 1}")
                html += f"""
  <div class="print-room">
    <div class="room-title">{room_name}</div>
    {build_mfr_html(e)}
    {build_info_line(e, True)}
    {build_spec_columns(e, True)}
    <hr class="print-notes-separator">
    <div class="print-room-notes"><span class="notes-label">Notes:</span> <span class="notes-text">{notes}</span></div>
  </div>"""

        if conds:
            html += "<h2>Condensers</h2>"
            for i, c in enumerate(conds):
                notes = c.get("room-notes", "") or "—"
                room_name = c.get("room-name", f"Condenser {i + 1}")
                html += f"""
  <div class="print-room">
    <div class="room-title">{room_name}</div>
    {build_mfr_html(c)}
    {build_info_line(c, False)}
    {build_spec_columns(c, False)}
    <hr class="print-notes-separator">
    <div class="print-room-notes"><span class="notes-label">Notes:</span> <span class="notes-text">{notes}</span></div>
  </div>"""

        html += "</div></body></html>"

        pdf_buffer = io.BytesIO()
        HTML(string=html).write_pdf(pdf_buffer)
        pdf_buffer.seek(0)

        safe_customer = "".join(c for c in customer if c.isalnum() or c in " -_").strip()
        safe_date = visit_date.replace("/", "-").replace(" ", "_") if visit_date else datetime.now().strftime("%Y-%m-%d")
        filename = f"Print View - {safe_customer} - {safe_date}.pdf"

        return send_file(
            pdf_buffer,
            mimetype="application/pdf",
            as_attachment=True,
            download_name=filename,
        )

    except Exception as e:
        logger.exception("Error generating PDF")
        return jsonify({"error": str(e)}), 500


