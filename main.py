import os
import sys
import time
import json
import re
import logging
import calendar
import urllib.parse
from datetime import datetime, date
from logging.handlers import RotatingFileHandler
from pathlib import Path

from botasaurus.request import request, Request
from notifier import send_email

# Logging configuration
LOG_DIR = os.environ.get("LOG_DIR", "/app/logs")
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
LOG_FILE = Path(LOG_DIR) / "scraper.log"

logger = logging.getLogger("reservations_scraper")
logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler(str(LOG_FILE), maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
logger.addHandler(file_handler)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(console_handler)

logger.info("Logger initialized, writing to %s", LOG_FILE)

def _parse_iso_date(s: str) -> date:
    """Parse YYYY-MM-DD. Raise ValueError on bad format."""
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()

def compute_3_month_window(reservation_start: str, reservation_end: str | None = None):
    """
    Given reservation_start (YYYY-MM-DD) and optional reservation_end (YYYY-MM-DD),
    compute:
      START_DATE = first day of month containing reservation_start
      END_DATE   = last day of the month that is START_DATE + 2 months (3-month span)
    Returns tuple of formatted strings like "Sun Feb 01 2026".
    Logs a warning if reservation_end is provided and lies outside the computed window.
    """
    rs = _parse_iso_date(reservation_start)
    # window start = first day of month containing rs
    start = date(rs.year, rs.month, 1)

    # compute month +2 (i.e., third month)
    month = start.month - 1 + 2  # +2 months ahead (0-index adjustment)
    year = start.year + month // 12
    month = month % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    end = date(year, month, last_day)

    if reservation_end:
        try:
            re_date = _parse_iso_date(reservation_end)
            if re_date < start or re_date > end:
                logger.warning("Provided RESERVATION_END_DATE (%s) is outside computed 3-month window %s -> %s",
                               reservation_end, start.isoformat(), end.isoformat())
        except Exception:
            logger.warning("Unable to parse RESERVATION_END_DATE (%s); expected YYYY-MM-DD", reservation_end)

    # Format to "Sun Feb 01 2026"
    fmt = lambda d: d.strftime("%a %b %d %Y")
    return fmt(start), fmt(end)

def find_availability_items(obj):
    matches = []
    if isinstance(obj, dict):
        keys = {k.lower(): k for k in obj.keys()}
        if "availablecount" in keys and ("date" in keys or "day" in keys):
            matches.append(obj)
        else:
            for v in obj.values():
                matches.extend(find_availability_items(v))
    elif isinstance(obj, list):
        for el in obj:
            matches.extend(find_availability_items(el))
    return matches

@request(max_retry=10)
def scrape_heading_task(request: Request, data):
    url = "https://reservations.ahlsmsworld.com/Yosemite/Search/GetInventoryCountData"

    # Compute START_DATE / END_DATE from RESERVATION_START_DATE and RESERVATION_END_DATE
    res_start = (data or {}).get("RESERVATION_START_DATE") or os.environ.get("RESERVATION_START_DATE")
    res_end = (data or {}).get("RESERVATION_END_DATE") or os.environ.get("RESERVATION_END_DATE")

    if not res_start:
        logger.error("Missing RESERVATION_START_DATE (env or data). Expected YYYY-MM-DD")
        return {"error": "missing_reservation_start_date", "message": "Set RESERVATION_START_DATE in env or data (YYYY-MM-DD)"}

    try:
        start_date_str, end_date_str = compute_3_month_window(res_start, res_end)
    except Exception as exc:
        logger.exception("Failed to compute 3-month window: %s", exc)
        return {"error": "bad_reservation_dates", "exception": str(exc)}

    params = {
        "callback": "$.wxa.on_datepicker_general_availability_loaded",
        "CresPropCode": "000000",
        "MultiPropCode": os.environ.get("HOTEL", "Y"),
        "UnitTypeCode": "",
        "StartDate": start_date_str,
        "EndDate": end_date_str,
        "_": str(int(time.time() * 1000)),
    }

    headers = {
        "User-Agent": os.environ.get("USER_AGENT", "Mozilla/5.0"),
        "Accept": "*/*",
#        "Referer": "https://reservations.ahlsmsworld.com/",
    }

    # --- log the prepared request being used to scrape ---
    try:
        params_snippet = json.dumps(params, ensure_ascii=False)
    except Exception:
        params_snippet = str(params)
    try:
        headers_snippet = json.dumps(headers, ensure_ascii=False)
    except Exception:
        headers_snippet = str(headers)

    try:
        final_url = f"{url}?{urllib.parse.urlencode(params, doseq=True)}"
    except Exception:
        final_url = url

    logger.info("Prepared GET request for scraping")
    logger.debug("Request URL: %s", final_url)
    logger.debug("Request params: %s", params_snippet)
    logger.debug("Request headers: %s", headers_snippet)
    # --- end request log ---

    logger.info("Requesting inventory (%s -> %s)", params["StartDate"], params["EndDate"])
    response = request.get(url, params=params, headers=headers)

    # Log details from the actual response/request if available
    try:
        resp_status = getattr(response, "status_code", None)
        req_obj = getattr(response, "request", None)
        if req_obj is not None:
            req_method = getattr(req_obj, "method", "GET")
            req_url = getattr(req_obj, "url", None) or final_url
            logger.info("HTTP %s %s -> status %s", req_method, req_url, resp_status)
        else:
            logger.info("HTTP GET %s -> status %s", final_url, resp_status)
    except Exception:
        logger.debug("Unable to log response.request details", exc_info=True)

    text = getattr(response, "text", None)
    if text is None:
        content = getattr(response, "content", None)
        text = content.decode("utf-8", errors="replace") if content is not None else str(response)

    status = getattr(response, "status_code", None)
    headers_resp = getattr(response, "headers", {})

    logger.info("Response status=%s content-type=%s length=%s", status, headers_resp.get("Content-Type"), len(text) if text else 0)

    m = re.search(r'^[^(]*\(\s*([\[\{].*[\]\}])\s*\)\s*;?\s*$', text, re.S)
    if m:
        candidate = m.group(1)
    else:
        first_obj_pos = min([p for p in (text.find("{"), text.find("[")) if p != -1], default=-1)
        if first_obj_pos == -1:
            raw_path = Path(LOG_DIR) / "raw_response.txt"
            try:
                raw_path.write_text(text or "", encoding="utf-8")
                logger.warning("No JSON found in response, saved raw_response to %s", raw_path)
            except Exception as e:
                logger.exception("Failed to write raw_response.txt: %s", e)
            return {"error": "no_json_found", "message": f"raw saved to {raw_path}"}
        else:
            last_obj = text.rfind("}") if text[first_obj_pos] == "{" else text.rfind("]")
            candidate = text[first_obj_pos:last_obj + 1]

    try:
        payload = json.loads(candidate)
    except Exception as exc:
        raw_path = Path(LOG_DIR) / "raw_response.txt"
        try:
            raw_path.write_text(text or "", encoding="utf-8")
            logger.exception("JSON parse failed, candidate snippet saved to %s", raw_path)
        except Exception as e:
            logger.exception("Failed to write raw_response.txt: %s", e)
        return {"error": "json_parse_failed", "exception": str(exc), "candidate_snippet": candidate[:2000]}

    logger.info("Parsed payload successfully")
    return {"inventory": payload}

def check_and_alert(result):
    """
    Check availability for the exact window defined by RESERVATION_START_DATE and
    RESERVATION_END_DATE (expected YYYY-MM-DD). Detailed debug logging included.
    """
    logger.debug("check_and_alert called; result payload type=%s", type(result.get("inventory")).__name__)

    payload = result.get("inventory")
    if not payload:
        logger.info("No payload found in result")
        return {"status": "no_payload"}

    # Build dict: iso_date -> AvailableCount
    counts = {}
    if isinstance(payload, list):
        for idx, item in enumerate(payload, start=1):
            if not isinstance(item, dict):
                logger.debug("Skipping non-dict inventory item %d: %r", idx, item)
                continue
            date_key = next((k for k in item.keys() if k.lower() in ("datekey", "date", "day", "date_key")), None)
            ac_key = next((k for k in item.keys() if k.lower() == "availablecount"), None)
            if not date_key:
                logger.debug("Item %d missing date key, skipping: %r", idx, item)
                continue
            date_val = item.get(date_key)
            if date_val is None:
                logger.debug("Item %d date value is None, skipping: %r", idx, item)
                continue
            iso = str(date_val).strip()
            raw_ac = item.get(ac_key) if ac_key else None
            try:
                if raw_ac is None:
                    ac = 0
                elif isinstance(raw_ac, (int, float)):
                    ac = int(raw_ac)
                else:
                    s = str(raw_ac).strip()
                    try:
                        ac = int(s)
                    except Exception:
                        try:
                            ac = int(float(s))
                        except Exception:
                            import re
                            m = re.search(r"(-?\d+)", s)
                            ac = int(m.group(1)) if m else 0
            except Exception as e:
                logger.debug("Failed to parse AvailableCount for item %d (%s): %s", idx, iso, e)
                ac = 0
            counts[iso] = ac
            logger.debug("Mapped inventory: %s -> %d", iso, ac)
    elif isinstance(payload, dict):
        # try nested common list keys
        for key in ("inventory", "data", "items", "days"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return check_and_alert({"inventory": candidate})
        logger.debug("Payload is dict; attempting to interpret top-level keys as date->count")
        for k, v in payload.items():
            try:
                counts[str(k)] = int(v)
            except Exception:
                logger.debug("Skipping key %s with non-int value %r", k, v)
    else:
        logger.debug("Unsupported payload type: %s", type(payload))

    # Read exact window from env
    res_start_iso = os.environ.get("RESERVATION_START_DATE")
    res_end_iso = os.environ.get("RESERVATION_END_DATE")
    if not res_start_iso or not res_end_iso:
        logger.error("RESERVATION_START_DATE and RESERVATION_END_DATE must be set (YYYY-MM-DD)")
        return {"status": "missing_reservation_window"}

    try:
        start = _parse_iso_date(res_start_iso)
        end = _parse_iso_date(res_end_iso)
    except Exception as exc:
        logger.exception("Failed to parse reservation window start/end: %s / %s", res_start_iso, res_end_iso)
        return {"status": "bad_reservation_dates", "exception": str(exc)}

    if end < start:
        logger.error("RESERVATION_END_DATE < RESERVATION_START_DATE (%s > %s)", res_end_iso, res_start_iso)
        return {"status": "bad_reservation_dates", "message": "end < start"}

    logger.info("Checking availability for exact window %s -> %s", start.isoformat(), end.isoformat())

    from datetime import timedelta
    d = start
    missing_dates = []
    non_positive = []
    total_checked = 0
    while d <= end:
        iso = d.isoformat()
        total_checked += 1
        ac = counts.get(iso)
        logger.debug("Date %s -> AvailableCount=%r", iso, ac)
        if ac is None:
            missing_dates.append(iso)
            logger.debug("Missing inventory for date %s", iso)
        else:
            try:
                if int(ac) <= 0:
                    non_positive.append({"date": iso, "AvailableCount": int(ac)})
                    logger.debug("Date %s has non-positive count=%s", iso, ac)
            except Exception:
                non_positive.append({"date": iso, "AvailableCount": ac})
                logger.debug("Date %s has unparsable count=%r", iso, ac)
        d += timedelta(days=1)

    logger.debug("Checked %d dates; missing=%d non_positive=%d", total_checked, len(missing_dates), len(non_positive))

    if missing_dates:
        logger.info("Cannot alert: missing inventory for %d dates (examples: %s)", len(missing_dates), missing_dates[:5])
        return {"status": "no_inventory_for_dates", "missing_dates_count": len(missing_dates), "missing_examples": missing_dates[:10]}

    if non_positive:
        logger.info("Availability found but not all dates positive; non-positive date(s): %s", non_positive[:10])
        return {"status": "not_all_positive", "non_positive_dates": non_positive[:10]}

    # All dates present and > 0
    logger.info("All %d dates in window have AvailableCount > 0", total_checked)
    body_lines = [f"All {total_checked} days in window {start.isoformat()} -> {end.isoformat()} have AvailableCount > 0:"]
    d = start
    while d <= end:
        iso = d.isoformat()
        body_lines.append(f"- {iso}: {counts.get(iso)}")
        d += timedelta(days=1)
    body = "\n".join(body_lines)

    # Create counts only for the reservation window
    window_counts = {}
    d = start
    while d <= end:
        iso = d.isoformat()
        window_counts[iso] = counts.get(iso, 0)
        d += timedelta(days=1)

    # Load previous counts to check for changes
    previous_path = Path(LOG_DIR) / "previous_counts.json"
    try:
        with open(previous_path, 'r', encoding='utf-8') as f:
            previous_counts = json.load(f)
    except FileNotFoundError:
        previous_counts = None
    except Exception as e:
        logger.warning("Failed to load previous counts: %s", e)
        previous_counts = None

    if previous_counts == window_counts:
        logger.info("Available counts unchanged since last check, skipping email")
        return {"status": "unchanged", "count": total_checked}

    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", 587))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    email_to = os.environ.get("EMAIL_TO")

    logger.debug("SMTP config present: host=%s user=%s email_to=%s", bool(smtp_host), bool(smtp_user), bool(email_to))

    if smtp_host and smtp_user and smtp_password and email_to:
        try:
            send_email(
                smtp_host=smtp_host,
                smtp_port=smtp_port,
                smtp_user=smtp_user,
                smtp_password=smtp_password,
                to_addrs=[s.strip() for s in email_to.split(",")],
                subject=f"Reservations availability: all days {start.isoformat()} -> {end.isoformat()} > 0",
                body=body,
                use_starttls=(smtp_port != 465),
            )
            logger.info("Alert email sent to %s", email_to)
            # Save current window counts as previous
            try:
                with open(previous_path, 'w', encoding='utf-8') as f:
                    json.dump(window_counts, f, ensure_ascii=False, indent=2)
                logger.debug("Saved previous counts")
            except Exception as e:
                logger.warning("Failed to save previous counts: %s", e)
            return {"status": "alert_sent", "count": total_checked}
        except Exception as exc:
            logger.exception("Failed to send alert email: %s", exc)
            return {"status": "alert_failed", "exception": str(exc)}
    else:
        logger.info("SMTP not configured; logging availability body")
        logger.info("%s", body)
        # Save current window counts even without SMTP
        try:
            with open(previous_path, 'w', encoding='utf-8') as f:
                json.dump(window_counts, f, ensure_ascii=False, indent=2)
            logger.debug("Saved previous counts")
        except Exception as e:
            logger.warning("Failed to save previous counts: %s", e)
        return {"status": "found_but_no_smtp", "count": total_checked, "details": "logged"}

def fetch_with_retries(data, max_attempts=3, delay_seconds=5):
    """
    Attempt to scrape and check for payload up to max_attempts.
    If check_and_alert reports status == 'no_payload', retry after delay_seconds.
    Returns the final (result, alert_result) tuple.
    """
    attempt = 1
    last_result = None
    last_alert = None

    while attempt <= max_attempts:
        logger.info("Fetch attempt %d/%d", attempt, max_attempts)
        last_result = scrape_heading_task(data=data)
        # write intermediate output for inspection
        out_path = Path(LOG_DIR) / f"output.json"
        try:
            out_path.write_text(json.dumps(last_result, indent=2, ensure_ascii=False), encoding="utf-8")
            logger.debug("Wrote %s", out_path)
        except Exception:
            logger.exception("Failed to write interim output file on attempt %d", attempt)

        last_alert = check_and_alert(last_result)
        logger.debug("Attempt %d check result: %s", attempt, last_alert)

        alert_status = last_alert.get("status")
        if alert_status != "no_payload":
            # terminal status (success, error, or no inventory) - stop retrying
            logger.info("Terminal status '%s' received; stopping retry loop", alert_status)
            break

        # if we have more attempts, sleep then retry
        attempt += 1
        if attempt <= max_attempts:
            logger.warning("No payload found, retrying in %s seconds (attempt %d/%d)", delay_seconds, attempt, max_attempts)
            time.sleep(delay_seconds)
        else:
            logger.error("No payload after %d attempts", max_attempts)

    return last_result, last_alert

def run_once():
    data = {
        # allow overriding via task data as well as env vars
        "RESERVATION_START_DATE": os.environ.get("RESERVATION_START_DATE"),
        "RESERVATION_END_DATE": os.environ.get("RESERVATION_END_DATE"),
    }

    # perform fetch with retries when check returns no_payload
    final_result, final_alert = fetch_with_retries(data=data, max_attempts=3, delay_seconds=5)

    # write final output files (overwrite main names)
    out_path = Path(LOG_DIR) / "output.json"
    try:
        out_path.write_text(json.dumps(final_result, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("Wrote final output.json to %s", out_path)
    except Exception as e:
        logger.exception("Failed to write output.json: %s", e)

    alert_path = Path(LOG_DIR) / "alert_result.json"
    try:
        alert_path.write_text(json.dumps(final_alert, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info("Wrote final alert_result.json to %s", alert_path)
    except Exception as e:
        logger.exception("Failed to write alert_result.json: %s", e)

    logger.info("Run result: %s", final_alert)

if __name__ == "__main__":
    interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "0"))
    if interval <= 0:
        run_once()
    else:
        try:
            while True:
                run_once()
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Interrupted by user")