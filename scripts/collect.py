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
import pg8000.dbapi
from google.cloud.sql.connector import Connector

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
# Cloud SQL Python Connector 사용 (IP 허용 불필요, 서비스 계정 IAM 인증)
INSTANCE_CONNECTION_NAME = os.environ.get(
    "INSTANCE_CONNECTION_NAME",
    "aboutfishingproduct:asia-northeast3:aboutfishing-svc-postgres-v2"
)
DB_NAME = os.environ.get("DB_NAME", "aboutfishing_db")
DB_USER = os.environ.get("DB_USER", "fisherman")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")  # GitHub Actions Secret

# 전역 Connector 인스턴스
_connector: Connector = None

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
    """Cloud SQL Python Connector + pg8000으로 안전하게 연결 (IP 허용 불필요)"""
    global _connector
    if not DB_PASSWORD:
        log.error("DB_PASSWORD 환경변수가 설정되지 않았습니다.")
        sys.exit(1)
    _connector = Connector()
    conn = _connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
    )
    log.info(f"Cloud SQL Connector(pg8000) 연결 성공: {INSTANCE_CONNECTION_NAME}")
    return conn


def setup_db(conn):
    """테이블이 없으면 생성 (pg8000 호환)"""
    cur = conn.cursor()
    try:
        # pg8000은 세미콜론 포함 multi-statement 불가 → 각각 실행
        for stmt in CREATE_TABLE_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        for stmt in CREATE_HISTORY_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        conn.commit()
    finally:
        cur.close()
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
def deduplicate_rows(rows: list) -> list:
    """
    고유 제약 조건 (ship_name, fishing_date, depart_time, schedule_no) 기준으로 중복 제거.
    동일 키가 여러 번 등장하면 마지막(최신) 값을 유지.
    인덱스: ship_name=1, fishing_date=4, depart_time=5, schedule_no=15
    """
    seen = {}
    for row in rows:
        key = (row[1], row[4], row[5], row[15])
        seen[key] = row  # 동일 키 있으면 마지막 값으로 덮어씀
    deduped = list(seen.values())
    if len(deduped) < len(rows):
        log.info(f"중복 제거: {len(rows)}행 → {len(deduped)}행 ({len(rows) - len(deduped)}개 중복 제거)")
    return deduped


def save_to_postgres(rows: list, conn):
    """PostgreSQL에 배치 UPSERT (pg8000 다중 VALUES 방식 — 1 SQL per batch)"""
    if not rows:
        log.warning("저장할 데이터가 없습니다.")
        return 0

    # ── 중복 제거 (동일 배치 내 같은 고유 키 → ON CONFLICT 에러 방지) ──
    original_count = len(rows)
    rows = deduplicate_rows(rows)

    BATCH_SIZE = 500
    COLS = 16

    cur = conn.cursor()
    try:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            # 단일 INSERT + 다중 VALUES (executemany 대비 ~100배 빠름)
            placeholders = ",".join(
                ["(" + ",".join(["%s"] * COLS) + ")"] * len(batch)
            )
            flat_params = [v for row in batch for v in row]
            cur.execute(
                f"""
                INSERT INTO fishing_seat_status (
                    collected_at, ship_name, area_name, port_name, fishing_date,
                    depart_time, return_time, fish_types, fishing_methods, price,
                    total_seats, reserved_seats, remain_seats,
                    status_code, status_name, schedule_no
                ) VALUES {placeholders}
                ON CONFLICT (ship_name, fishing_date, depart_time, schedule_no)
                DO UPDATE SET
                    collected_at   = EXCLUDED.collected_at,
                    remain_seats   = EXCLUDED.remain_seats,
                    reserved_seats = EXCLUDED.reserved_seats,
                    status_code    = EXCLUDED.status_code,
                    status_name    = EXCLUDED.status_name,
                    area_name      = EXCLUDED.area_name,
                    port_name      = EXCLUDED.port_name,
                    return_time    = EXCLUDED.return_time,
                    fish_types     = EXCLUDED.fish_types,
                    fishing_methods = EXCLUDED.fishing_methods,
                    price          = EXCLUDED.price,
                    total_seats    = EXCLUDED.total_seats
                """,
                flat_params,
            )
            log.info(f"  배치 저장: {i+len(batch)}/{len(rows)}행")

        # 수집 이력 저장
        cur.execute(
            "INSERT INTO fishing_collect_history (collected_at, row_count, status, note) "
            "VALUES (NOW(), %s, %s, %s)",
            (len(rows), "성공", f"수집 {original_count}행, 중복제거 후 {len(rows)}행, {DAYS_AHEAD}일치"),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.error(f"DB 저장 실패: {e}")
        # 실패 이력도 기록
        cur2 = conn.cursor()
        try:
            cur2.execute(
                "INSERT INTO fishing_collect_history (collected_at, row_count, status, note) "
                "VALUES (NOW(), %s, %s, %s)",
                (0, "실패", str(e)[:200]),
            )
            conn.commit()
        finally:
            cur2.close()
        raise
    finally:
        cur.close()

    log.info(f"PostgreSQL 저장 완료: {len(rows)}행 UPSERT (수집 원본: {original_count}행)")
    return len(rows)


# ── 메인 ──────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("선상24 좌석 데이터 수집 시작 (PostgreSQL + asyncio v2)")
    log.info(f"날짜 범위: 오늘({date.today()}) ~ +{DAYS_AHEAD}일")
    log.info(f"DB: {INSTANCE_CONNECTION_NAME}/{DB_NAME}")
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
    if _connector:
        _connector.close()

    elapsed = (datetime.now() - start).total_seconds()
    log.info("=" * 60)
    log.info(f"수집 완료! 소요시간: {elapsed:.1f}초 ({elapsed/60:.1f}분)")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
