#!/usr/bin/env python3
"""
선상24 낚시 예약 데이터 수집 스크립트
- app.sunsang24.com/ship/list API에서 전국 선상 좌석 현황 수집
- Google Sheets에 결과 저장
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, date, timedelta

import requests
import gspread
from google.oauth2.service_account import Credentials

# ── 로깅 설정 ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── 상수 ──────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://app.sunsang24.com/ship/list",
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

BASE_URL = "https://app.sunsang24.com/ship/list"

# Google Sheets 설정
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# 수집할 날짜 범위 (오늘 포함 14일)
DAYS_AHEAD = 14

# 시트 헤더 컬럼
SHEET_HEADERS = [
    "수집일시",       # 데이터 수집 시각
    "선박명",         # 선박 이름
    "지역",           # 지역 (남해안/서해안/동해안/제주도)
    "항구",           # 출발 항구
    "출조일",         # 날짜 (YYYY-MM-DD)
    "출항시간",       # 출발 시간
    "철수시간",       # 종료 시간
    "상품내용1(어종)",       # 어종 목록
    "상품내용2(낚시방법)",   # 낚시 방법
    "상품내용3(선비)",       # 선비 (원)
    "총정원",         # 총 정원 수
    "예약인원",       # 예약 완료 인원
    "남은좌석",       # 남은 좌석 ★ 핵심
    "예약상태",       # ING / END / CALL
    "상태명",         # 예약가능 / 예약마감 / 전화예약
    "스케줄번호",     # ship_schedule_no (상세 조회용)
]


# ── 데이터 수집 함수 ───────────────────────────────────────

def fetch_ships_page(target_date: str, page: int, area: str = "all") -> list:
    """단일 페이지 선상 목록 조회"""
    params = {
        "mode": "json",
        "page": page,
        "search": "y",
        "f": "",
        "ad": "",
        "area": area,
        "sort": "date",
        "lat": "",
        "long": "",
        "sk": "",
        "s": target_date,
    }
    try:
        resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        log.warning(f"페이지 {page} 조회 실패: {e}")
        return []


def fetch_all_ships(target_date: str) -> list:
    """전체 페이지 순회하여 모든 선상 수집"""
    all_ships = []
    page = 1
    while True:
        ships = fetch_ships_page(target_date, page)
        if not ships:
            break
        all_ships.extend(ships)
        log.info(f"  [날짜:{target_date}] 페이지 {page} → {len(ships)}척 수집")
        page += 1
        time.sleep(0.4)  # 서버 부하 방지
    return all_ships


def collect_data(days_ahead: int = DAYS_AHEAD) -> list:
    """오늘부터 days_ahead일치 데이터 수집"""
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for i in range(days_ahead):
        target_date = (date.today() + timedelta(days=i)).isoformat()
        log.info(f"[{i+1}/{days_ahead}] {target_date} 수집 시작...")
        ships = fetch_all_ships(target_date)

        for ship in ships:
            fish_types = ", ".join(
                [f["name"] for f in ship.get("fish_type", [])]
            )
            fishing_methods = ", ".join(
                [m["name"] for m in ship.get("fishing_method", [])]
            )
            price = ship.get("price", 0)
            price_str = f"{price:,}원" if price else "전화문의"

            row = [
                collected_at,
                ship.get("name", ""),
                ship.get("area_name", ""),
                ship.get("port_name", ""),
                ship.get("sdate", ""),
                ship.get("stime", "")[:5] if ship.get("stime") else "",
                ship.get("etime", "")[:5] if ship.get("etime") else "",
                fish_types,           # 상품내용1: 어종
                fishing_methods,      # 상품내용2: 낚시방법
                price_str,            # 상품내용3: 선비
                ship.get("embarkation_num", 0),
                ship.get("reservation_ready_num", 0),
                ship.get("remain_embarkation_num", 0),   # ★ 남은 좌석
                ship.get("schedule_status_code", ""),
                ship.get("schedule_status_name", ""),
                ship.get("ship_schedule_no", ""),
            ]
            rows.append(row)

        log.info(f"  → {target_date}: {len(ships)}척 완료")

    log.info(f"총 {len(rows)}행 수집 완료")
    return rows


# ── Google Sheets 연동 ─────────────────────────────────────

def get_gspread_client() -> gspread.Client:
    """환경변수 또는 파일에서 인증 정보 로드"""
    # GitHub Actions: GOOGLE_SERVICE_ACCOUNT_JSON 환경변수
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if sa_json:
        import json as json_mod
        cred_dict = json_mod.loads(sa_json)
        creds = Credentials.from_service_account_info(cred_dict, scopes=SCOPES)
    else:
        # 로컬 개발: credentials.json 파일
        cred_file = os.path.join(os.path.dirname(__file__), "..", "credentials.json")
        if not os.path.exists(cred_file):
            raise FileNotFoundError(
                "credentials.json 파일이 없습니다. "
                "GCP 서비스 계정 키를 다운로드하거나 "
                "GOOGLE_SERVICE_ACCOUNT_JSON 환경변수를 설정하세요."
            )
        creds = Credentials.from_service_account_file(cred_file, scopes=SCOPES)

    return gspread.authorize(creds)


def write_to_sheets(rows: list, spreadsheet_id: str, sheet_name: str = "실시간좌석현황"):
    """Google Sheets에 데이터 저장"""
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(spreadsheet_id)

        # 시트 가져오기 또는 생성
        try:
            ws = sh.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=sheet_name, rows=5000, cols=len(SHEET_HEADERS))
            log.info(f"새 시트 '{sheet_name}' 생성")

        # 기존 데이터 초기화
        ws.clear()

        # 헤더 + 데이터 한 번에 쓰기
        all_data = [SHEET_HEADERS] + rows
        ws.update("A1", all_data, value_input_option="USER_ENTERED")

        # 헤더 서식 적용 (굵게, 배경색)
        header_format = {
            "backgroundColor": {"red": 0.12, "green": 0.46, "blue": 0.71},
            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "horizontalAlignment": "CENTER",
        }
        ws.format("A1:P1", header_format)

        # 열 너비 자동 조정 요청
        sh.batch_update({
            "requests": [{
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": ws.id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": len(SHEET_HEADERS),
                    }
                }
            }]
        })

        # 수집 이력 시트에도 기록
        _write_history(sh, len(rows))

        log.info(
            f"Google Sheets 업데이트 완료: "
            f"{len(rows)}행 → 시트 '{sheet_name}'"
        )
        return ws.url

    except Exception as e:
        log.error(f"Google Sheets 저장 실패: {e}")
        raise


def _write_history(sh: gspread.Spreadsheet, row_count: int):
    """수집 이력 시트 기록"""
    hist_name = "수집이력"
    try:
        hist = sh.worksheet(hist_name)
    except gspread.exceptions.WorksheetNotFound:
        hist = sh.add_worksheet(title=hist_name, rows=1000, cols=4)
        hist.update("A1", [["수집일시", "수집행수", "상태", "비고"]])

    hist.append_row([
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        row_count,
        "성공",
        f"오늘 포함 {DAYS_AHEAD}일치",
    ])


# ── 메인 ──────────────────────────────────────────────────

def main():
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "")
    if not spreadsheet_id:
        log.error("SPREADSHEET_ID 환경변수가 설정되지 않았습니다.")
        sys.exit(1)

    log.info("=" * 60)
    log.info("선상24 좌석 데이터 수집 시작")
    log.info(f"날짜 범위: 오늘({date.today()}) ~ +{DAYS_AHEAD}일")
    log.info("=" * 60)

    rows = collect_data(DAYS_AHEAD)
    write_to_sheets(rows, spreadsheet_id)

    log.info("=" * 60)
    log.info("수집 완료!")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
