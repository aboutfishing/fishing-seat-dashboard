#!/usr/bin/env python3
"""
선상24 낚시 예약 데이터 수집 스크립트 v2 (PostgreSQL + asyncio 버전)
- asyncio + aiohttp 병렬 수집으로 수집 시간 대폭 단축 (예상 2~3분)
- GCP Cloud SQL PostgreSQL에 배치 저장
- Google Sheets 대비 약 5~10배 빠른 수집
"""

import asyncio
import os
import sys
import json
import logging
from datetime import datetime, date, timedelta

import aiohttp
import psycopg2
from psycopg2.extras import execute_values

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

# 수집할 날짜 범위 (오늘 포함 14일)
DAYS_AHEAD = 14

# 동시 요청 수 제한 (서버 부하 방지)
MAX_CONCURRENT = 10

# ── DB 연결 정보 ────────────────────────────────────────────
# GitHub Actions Secrets 또는 환경변수로 주입
DB_HOST = os.environ.get("DB_HOST", "34.47.87.149")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "aboutfishing_db")
DB_USER = os.environ.get("DB_USER", "fisherman")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")  # GitHub Actions Secret

# ── DB 테이블 DDL ───────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fishing_seat_status (
    id               SERIAL PRIMARY KEY,
    collected_at     TIMESTAMP NOT NULL,
    ship_name        VARCHAR(100),
    area_name        VARCHAR(50),
    port_name        VARCHAR(100),
    fishing_date     DATE,
    depart_time      VARCHAR(10),
    return_time      VARCHAR(10),
    fish_types       TEXT,
    fishing_methods  TEXT,
    price            VARCHAR(50),
    total_seats      INTEGER DEFAULT 0,
    reserved_seats   INTEGER DEFAULT 0,
    remain_seats     INTEGER DEFAULT 0,
    status_code      VARCHAR(20),
    status_name      VARCHAR(50),
    schedule_no      VARCHAR(50),
    UNIQUE (ship_name, fishing_date, depart_time, schedule_no)
);

CREATE INDEX IF NOT EXISTS idx_fishing_date ON fishing_seat_status (fishing_date);
CREATE INDEX IF NOT EXISTS idx_collected_at ON fishing_seat_status (collected_at);
CREATE INDEX IF NOT EXISTS idx_area ON fishing_seat_status (area_name);
CREATE INDEX IF NOT EXISTS idx_remain ON fishing_seat_status (remain_seats);
"""

UPSERT_SQL = """
INSERT INTO fishing_seat_status (
    collected_at, ship_name, area_name, port_name, fishing_date,
    depart_time, return_time, fish_types, fishing_methods, price,
    total_seats, reserved_seats, remain_seats,
    status_code, status_name, schedule_no
) VALUES %s
ON CONFLICT (ship_name, fishing_date, depart_time, schedule_no)
DO UPDATE SET
    collected_at    = EXCLUDED.collected_at,
    remain_seats    = EXCLUDED.remain_seats,
    reserved_seats  = EXCLUDED.reserved_seats,
    status_code     = EXCLUDED.status_code,
    status_name     = EXCLUDED.status_name;
"""

# ── 수집 이력 테이블 ────────────────────────────────────────
CREATE_HISTORY_SQL = """
CREATE TABLE IF NOT EXISTS fishing_collect_history (
    id           SERIAL PRIMARY KEY,
    collected_at TIMESTAMP DEFAULT NOW(),
    row_count    INTEGER,
    status       VARCHAR(20),
    note         TEXT
);
"""


# ── DB 연결 ─────────────────────────────────────────────────
def get_db_connection():
    if not DB_PASSWORD:
        log.error("DB_PASSWORD 환경변수가 설정되지 않았습니다.")
        sys.exit(1)
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=10,
        sslmode="prefer",
    )


def setup_db(conn):
    """테이블이 없으면 생성"""
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
        cur.execute(CREATE_HISTORY_SQL)
    conn.commit()
    log.info("DB 테이블 준비 완료")


# ── 비동기 수집 함수 ────────────────────────────────────────
async def fetch_page(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore,
                     target_date: str, page: int) -> list:
    """단일 페이지 비동기 조회"""
    params = {
        "mode": "json", "page": page, "search": "y", "f": "", "ad": "",
        "area": "all", "sort": "date", "lat": "", "long": "", "sk": "",
        "s": target_date,
    }
    async with semaphore:
        try:
            async with session.get(BASE_URL, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
                return data if isinstance(data, list) else []
        except Exception as e:
            log.warning(f"  페이지 {page} ({target_date}) 조회 실패: {e}")
            return []


async def fetch_all_pages_for_date(session: aiohttp.ClientSession,
                                    semaphore: asyncio.Semaphore,
                                    target_date: str) -> list:
    """한 날짜의 전체 페이지를 20페이지씩 배치로 병렬 조회 (무한 페이지 대응)"""
    all_ships = []
    page = 1
    BATCH = 20  # 한 번에 20페이지씩 병렬 요청

    while True:
        page_range = range(page, page + BATCH)
        tasks = [fetch_page(session, semaphore, target_date, p) for p in page_range]
        results = await asyncio.gather(*tasks)

        batch_ships = []
        found_empty = False
        for data in results:
            if not data:
                found_empty = True
                break
            batch_ships.extend(data)

        all_ships.extend(batch_ships)

        if found_empty or not batch_ships:
            break  # 빈 페이지 나오면 해당 날짜 완료

        page += BATCH

    log.info(f"  [{target_date}] {len(all_ships)}척 수집 (페이지 {page + BATCH - 1}까지)")
    return all_ships


async def collect_all_async() -> list:
    """14일치 데이터 비동기 병렬 수집"""
    collected_at = datetime.now()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    dates = [
        (date.today() + timedelta(days=i)).isoformat()
        for i in range(DAYS_AHEAD)
    ]

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        # 모든 날짜 병렬 수집
        tasks = [
            fetch_all_pages_for_date(session, semaphore, d)
            for d in dates
        ]
        date_results = await asyncio.gather(*tasks)

    rows = []
    for ships in date_results:
        for ship in ships:
            fish_types = ", ".join([f["name"] for f in ship.get("fish_type", [])])
            fishing_methods = ", ".join([m["name"] for m in ship.get("fishing_method", [])])
            price = ship.get("price", 0)
            price_str = f"{price:,}원" if price else "전화문의"

            fishing_date_str = ship.get("sdate", "")
            try:
                fishing_date = date.fromisoformat(fishing_date_str) if fishing_date_str else None
            except ValueError:
                fishing_date = None

            rows.append((
                collected_at,
                ship.get("name", "")[:100],
                ship.get("area_name", "")[:50],
                ship.get("port_name", "")[:100],
                fishing_date,
                (ship.get("stime", "") or "")[:5],
                (ship.get("etime", "") or "")[:5],
                fish_types,
                fishing_methods,
                price_str[:50],
                ship.get("embarkation_num", 0) or 0,
                ship.get("reservation_ready_num", 0) or 0,
                ship.get("remain_embarkation_num", 0) or 0,
                ship.get("schedule_status_code", "")[:20],
                ship.get("schedule_status_name", "")[:50],
                str(ship.get("ship_schedule_no", ""))[:50],
            ))

    log.info(f"총 {len(rows)}행 수집 완료")
    return rows


# ── DB 저장 ─────────────────────────────────────────────────
def save_to_postgres(rows: list, conn):
    """PostgreSQL에 배치 UPSERT"""
    if not rows:
        log.warning("저장할 데이터가 없습니다.")
        return 0

    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=500)
        affected = cur.rowcount

    # 수집 이력 저장
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO fishing_collect_history (collected_at, row_count, status, note) "
            "VALUES (NOW(), %s, %s, %s)",
            (len(rows), "성공", f"오늘 포함 {DAYS_AHEAD}일치"),
        )
    conn.commit()
    log.info(f"PostgreSQL 저장 완료: {len(rows)}행 UPSERT → 실제 변경 {affected}행")
    return len(rows)


# ── 메인 ──────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("선상24 좌석 데이터 수집 시작 (PostgreSQL + asyncio v2)")
    log.info(f"날짜 범위: 오늘({date.today()}) ~ +{DAYS_AHEAD}일")
    log.info(f"DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    log.info("=" * 60)

    start = datetime.now()

    # DB 연결 & 테이블 초기화
    conn = get_db_connection()
    setup_db(conn)

    # 비동기 수집
    rows = asyncio.run(collect_all_async())

    # PostgreSQL 저장
    save_to_postgres(rows, conn)
    conn.close()

    elapsed = (datetime.now() - start).total_seconds()
    log.info("=" * 60)
    log.info(f"수집 완료! 소요시간: {elapsed:.1f}초 ({elapsed/60:.1f}분)")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
