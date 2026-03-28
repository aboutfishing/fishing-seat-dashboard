#!/usr/bin/env python3
"""
GCS → data/latest.json 변환 스크립트

GCS 버킷(aboutfishing-seat-data)에서 최신 좌석 데이터를 다운로드하고
대시보드에서 사용하는 data/latest.json 형식으로 변환합니다.

사용법:
    python scripts/gcs_to_dashboard.py --bucket aboutfishing-seat-data \
        --output data/latest.json

GCS 구조:
    seat-data/YYYY-MM-DD.json  — 날짜별 수집 데이터
    index.json                 — 수집된 날짜 목록

latest.json 출력 형식 (index.html과 호환):
    {
        "updated_at": "2026-03-28T03:00:00+09:00",
        "row_count": N,
        "rows": [[collected_at, ship_name, area_name, port_name,
                   fishing_date, depart_time, return_time, fish_types,
                   fishing_methods, price, total_seats, reserved_seats,
                   remain_seats, status_code, status_name, schedule_no], ...]
    }
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))


# ── GCS 다운로드 ───────────────────────────────────────────────────────────────

def gcs_download(gcs_uri: str, dest: str) -> bool:
    """gsutil cp로 GCS 파일 다운로드."""
    cmd = ["gsutil", "cp", gcs_uri, dest]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.error("gsutil 오류: %s", result.stderr)
        return False
    return True


def get_latest_date(bucket: str) -> str | None:
    """index.json에서 최신 날짜를 반환합니다."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        tmp = f.name

    gcs_uri = f"gs://{bucket}/index.json"
    if not gcs_download(gcs_uri, tmp):
        log.warning("index.json 다운로드 실패, 오늘 날짜로 시도합니다.")
        return datetime.now(KST).strftime("%Y-%m-%d")

    with open(tmp) as f:
        index_data = json.load(f)
    os.unlink(tmp)

    # index.json이 {"dates": [...], "latest": "..."} 형태인 경우
    if isinstance(index_data, dict):
        if "latest" in index_data:
            return index_data["latest"]
        if "dates" in index_data and index_data["dates"]:
            return sorted(index_data["dates"])[-1]

    # index.json이 날짜 리스트인 경우 ["2026-03-28", ...]
    if isinstance(index_data, list) and index_data:
        return sorted(index_data)[-1]

    log.warning("index.json 형식을 인식할 수 없습니다: %s", type(index_data))
    return datetime.now(KST).strftime("%Y-%m-%d")


# ── 데이터 변환 ────────────────────────────────────────────────────────────────

def _join_list_field(field_value, key="name") -> str:
    """[{"name": "루어"}, {"name": "선상"}] → "루어,선상"."""
    if not field_value:
        return ""
    if isinstance(field_value, list):
        items = []
        for item in field_value:
            if isinstance(item, dict):
                items.append(str(item.get(key, "")))
            elif isinstance(item, str):
                items.append(item)
        return ",".join(i for i in items if i)
    if isinstance(field_value, str):
        return field_value
    return str(field_value)


def _safe_int(val, default=0) -> int:
    try:
        return int(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def record_to_row(rec: dict, collected_at: str) -> list:
    """
    GCS 레코드 한 건을 16컬럼 배열로 변환합니다.

    지원하는 필드명 패턴 (api.sunsang24.com / app.sunsang24.com 공통):
      ship_name    : name, ship_name
      area_name    : area_name
      port_name    : port_name
      fishing_date : sdate, fishing_date, date
      depart_time  : stime, depart_time, start_time
      return_time  : etime, return_time, end_time
      fish_types   : fish_type, fish_types, fishing_type
      fish_methods : fishing_method, fishing_methods
      price        : price
      total_seats  : embarkation_num, total_seats, capacity
      reserved     : reservation_end_num, reserved_seats, reserved
      remain       : remain_embarkation_num, remain_seats, available
      status_code  : schedule_status_code, status_code, status
      status_name  : schedule_status_name, status_name
      schedule_no  : ship_schedule_no, schedule_no, schedule_id
    """
    def get(*keys):
        for k in keys:
            v = rec.get(k)
            if v is not None:
                return v
        return None

    ship_name    = get("name", "ship_name") or ""
    area_name    = get("area_name") or ""
    port_name    = get("port_name") or ""
    fishing_date = get("sdate", "fishing_date", "date") or ""
    depart_time  = get("stime", "depart_time", "start_time") or ""
    return_time  = get("etime", "return_time", "end_time") or ""

    fish_types   = _join_list_field(get("fish_type", "fish_types", "fishing_type"))
    fish_methods = _join_list_field(get("fishing_method", "fishing_methods"))

    price        = get("price") or 0
    total_seats  = _safe_int(get("embarkation_num", "total_seats", "capacity"))
    reserved     = _safe_int(get("reservation_end_num", "reserved_seats", "reserved"))
    remain       = _safe_int(get("remain_embarkation_num", "remain_seats", "available"))
    status_code  = get("schedule_status_code", "status_code", "status") or ""
    status_name  = get("schedule_status_name", "status_name") or ""
    schedule_no  = str(get("ship_schedule_no", "schedule_no", "schedule_id") or "")

    return [
        collected_at,    # 0: collected_at
        ship_name,       # 1: ship_name
        area_name,       # 2: area_name
        port_name,       # 3: port_name
        fishing_date,    # 4: fishing_date
        depart_time,     # 5: depart_time
        return_time,     # 6: return_time
        fish_types,      # 7: fish_types
        fish_methods,    # 8: fishing_methods
        price,           # 9: price
        total_seats,     # 10: total_seats
        reserved,        # 11: reserved_seats
        remain,          # 12: remain_seats
        status_code,     # 13: status_code
        status_name,     # 14: status_name
        schedule_no,     # 15: schedule_no
    ]


def convert_gcs_json(raw: dict | list, collected_at: str) -> list[list]:
    """
    GCS JSON 데이터를 16컬럼 rows 리스트로 변환합니다.
    다양한 형식을 자동으로 감지합니다.
    """
    # 형식 1: 레코드 배열 직접
    if isinstance(raw, list):
        records = raw
    # 형식 2: {"data": [...], ...} 또는 {"records": [...], ...}
    elif isinstance(raw, dict):
        records = (
            raw.get("data") or
            raw.get("records") or
            raw.get("ships") or
            raw.get("items") or
            raw.get("results") or
            []
        )
        if not records:
            # 혹시 dict 자체가 {date: [records]} 형태인 경우
            for v in raw.values():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    records = v
                    break
    else:
        log.error("알 수 없는 GCS 데이터 형식: %s", type(raw))
        return []

    rows = []
    for rec in records:
        if isinstance(rec, dict):
            rows.append(record_to_row(rec, collected_at))
        else:
            log.warning("레코드가 dict가 아닙니다: %s", type(rec))

    return rows


# ── 멀티데이트 병합 ────────────────────────────────────────────────────────────

def get_all_available_dates(bucket: str) -> list[str]:
    """GCS에서 수집된 날짜 목록을 모두 가져옵니다."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        tmp = f.name

    gcs_uri = f"gs://{bucket}/index.json"
    if not gcs_download(gcs_uri, tmp):
        return []

    with open(tmp) as f:
        index_data = json.load(f)
    os.unlink(tmp)

    if isinstance(index_data, dict):
        dates = index_data.get("dates", [])
    elif isinstance(index_data, list):
        dates = index_data
    else:
        dates = []

    return sorted(dates)


# ── 메인 ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="GCS → data/latest.json 변환")
    parser.add_argument("--bucket", default="aboutfishing-seat-data",
                        help="GCS 버킷 이름 (기본: aboutfishing-seat-data)")
    parser.add_argument("--output", default="data/latest.json",
                        help="출력 파일 경로 (기본: data/latest.json)")
    parser.add_argument("--date", default=None,
                        help="수집할 날짜 (기본: index.json의 최신 날짜)")
    parser.add_argument("--all-dates", action="store_true",
                        help="모든 가용 날짜를 병합하여 출력")
    args = parser.parse_args()

    bucket = args.bucket
    collected_at = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    all_rows = []

    if args.all_dates:
        dates = get_all_available_dates(bucket)
        log.info("병합할 날짜: %s개 (%s)", len(dates), ", ".join(dates[:5]))
    else:
        target_date = args.date or get_latest_date(bucket)
        log.info("대상 날짜: %s", target_date)
        dates = [target_date]

    for target_date in dates:
        gcs_uri = f"gs://{bucket}/seat-data/{target_date}.json"
        log.info("다운로드: %s", gcs_uri)

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            tmp = f.name

        if not gcs_download(gcs_uri, tmp):
            log.warning("날짜 %s 데이터 스킵", target_date)
            continue

        with open(tmp, encoding="utf-8") as f:
            raw = json.load(f)
        os.unlink(tmp)

        rows = convert_gcs_json(raw, collected_at)
        log.info("날짜 %s: %d행 변환됨", target_date, len(rows))
        all_rows.extend(rows)

    if not all_rows:
        log.error("변환된 데이터가 없습니다.")
        sys.exit(1)

    # 중복 제거 (schedule_no 기준)
    seen = {}
    for row in all_rows:
        key = (row[1], row[4], row[5], row[15])  # ship_name, date, depart, schedule_no
        seen[key] = row
    deduped = list(seen.values())
    log.info("중복 제거: %d행 → %d행", len(all_rows), len(deduped))

    # 출력
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.now(KST).isoformat(),
        "row_count": len(deduped),
        "rows": deduped,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    log.info("저장 완료: %s (%d행)", args.output, len(deduped))


if __name__ == "__main__":
    main()
