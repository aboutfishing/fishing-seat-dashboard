#!/usr/bin/env python3
"""
어바웃피싱 2026 전체 데이터 수집 스크립트
- 오늘부터 2026-12-31까지 전체 날짜 수집
- 에러 발생 시 3회 자동 재시도 (지수 백오프)
- 날짜별 독립 수집 → 일부 실패해도 나머지 계속 진행
- DB 저장 + GitHub Pages용 latest.json 업데이트
"""

import asyncio
import os
import sys
import json
import logging
import traceback
from datetime import datetime, date, timedelta
from time import sleep

import aiohttp
import pg8000.dbapi
from google.cloud.sql.connector import Connector

# ── 로깅 ──────────────────────────────────────────────────
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
BASE_URL   = "https://app.sunsang24.com/ship/list"
MAX_CONCURRENT = 8    # 동시 요청 수 (서버 부하 방지 - 2026 bulk 수집에는 조금 더 보수적)
MAX_RETRIES    = 3    # 날짜별 최대 재시도 횟수
RETRY_DELAY    = 5    # 재시도 기본 대기(초) — 지수 백오프 적용
PAGE_DELAY     = 0.3  # 페이지 간 딜레이(초)
BATCH_SIZE     = 500
COLS           = 16

# ── DB 연결 정보 ────────────────────────────────────────────
INSTANCE_CONNECTION_NAME = os.environ.get(
    "INSTANCE_CONNECTION_NAME",
    "aboutfishingproduct:asia-northeast3:aboutfishing-svc-postgres-v2"
)
DB_NAME     = os.environ.get("DB_NAME",     "aboutfishing_db")
DB_USER     = os.environ.get("DB_USER",     "fisherman")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

_connector: Connector = None

# ── DB 연결 ─────────────────────────────────────────────────
def get_db_connection():
    global _connector
    _connector = Connector()
    def getconn():
        return _connector.connect(
            INSTANCE_CONNECTION_NAME,
            "pg8000",
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
        )
    import sqlalchemy
    pool = sqlalchemy.create_engine("postgresql+pg8000://", creator=getconn)
    raw = pool.raw_connection()
    return raw

def setup_db(conn):
    cur = conn.cursor()
    try:
        cur.execute("""
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
        CREATE INDEX IF NOT EXISTS idx_area ON fishing_seat_status (area_name);
        CREATE INDEX IF NOT EXISTS idx_remain ON fishing_seat_status (remain_seats);
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fishing_collect_history (
            id           SERIAL PRIMARY KEY,
            collected_at TIMESTAMP DEFAULT NOW(),
            row_count    INTEGER,
            status       VARCHAR(20),
            note         TEXT
        );
        """)
        conn.commit()
        log.info("DB 테이블 초기화 완료")
    finally:
        cur.close()

# ── 날짜 생성 ────────────────────────────────────────────────
def get_target_dates():
    """오늘부터 2026-12-31까지 날짜 리스트"""
    start = date.today()
    end   = date(2026, 12, 31)
    dates = []
    d = start
    while d <= end:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    log.info(f"수집 대상 날짜: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")
    return dates

# ── 단일 날짜 수집 (재시도 포함) ─────────────────────────────
async def fetch_date_with_retry(session, sem, target_date: str, attempt=1) -> list:
    """한 날짜의 전체 페이지 수집, 에러 시 재시도"""
    try:
        return await fetch_single_date(session, sem, target_date)
    except Exception as e:
        if attempt <= MAX_RETRIES:
            wait = RETRY_DELAY * (2 ** (attempt - 1))  # 지수 백오프: 5, 10, 20초
            log.warning(f"[{target_date}] 수집 실패 (시도 {attempt}/{MAX_RETRIES}), {wait}초 후 재시도: {e}")
            await asyncio.sleep(wait)
            return await fetch_date_with_retry(session, sem, target_date, attempt + 1)
        else:
            log.error(f"[{target_date}] 최종 실패 (재시도 {MAX_RETRIES}회 소진): {e}")
            return []  # 실패한 날짜는 빈 리스트 반환, 수집 계속

async def fetch_single_date(session, sem, target_date: str) -> list:
    """한 날짜의 전체 페이지 수집"""
    results = []
    page = 1
    async with sem:
        while True:
            params = {
                "mode": "json",
                "page": page,
                "search": "y",
                "area": "all",
                "sort": "date",
                "s": target_date,
            }
            try:
                async with session.get(
                    BASE_URL,
                    params=params,
                    headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status != 200:
                        log.debug(f"  [{target_date}] p{page} → HTTP {resp.status}")
                        break
                    data = await resp.json(content_type=None)
                    ships = data if isinstance(data, list) else data.get("data", data.get("list", []))
                    if not ships:
                        break
                    results.extend(ships)
                    if len(ships) < 10:  # 마지막 페이지
                        break
                    page += 1
                    await asyncio.sleep(PAGE_DELAY)
            except asyncio.TimeoutError:
                log.warning(f"  [{target_date}] p{page} Timeout")
                break
            except Exception as e:
                log.warning(f"  [{target_date}] p{page} Error: {e}")
                break
    return results

# ── 수집 메인 ────────────────────────────────────────────────
async def collect_all_2026():
    """2026 전체 날짜 비동기 수집"""
    target_dates = get_target_dates()
    collected_at  = datetime.now()
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    # 날짜를 청크로 나눠 처리 (메모리 관리 + 중간 저장)
    CHUNK_SIZE = 30  # 30일씩 처리
    all_rows = []

    connector_timeout = aiohttp.TCPConnector(limit=MAX_CONCURRENT, force_close=True)
    async with aiohttp.ClientSession(connector=connector_timeout) as session:
        for chunk_start in range(0, len(target_dates), CHUNK_SIZE):
            chunk = target_dates[chunk_start:chunk_start + CHUNK_SIZE]
            log.info(f"청크 처리 중: {chunk[0]} ~ {chunk[-1]} ({len(chunk)}일)")

            tasks = [fetch_date_with_retry(session, sem, d) for d in chunk]
            chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

            chunk_rows = []
            for i, result in enumerate(chunk_results):
                if isinstance(result, Exception):
                    log.error(f"[{chunk[i]}] gather 예외: {result}")
                    continue
                if not result:
                    continue
                for ship in result:
                    row = parse_ship_row(ship, collected_at)
                    if row:
                        chunk_rows.append(row)

            all_rows.extend(chunk_rows)
            log.info(f"  → 청크 소계: {len(chunk_rows)}행 (누적: {len(all_rows)}행)")

            # 청크마다 짧은 대기 (서버 부하 방지)
            if chunk_start + CHUNK_SIZE < len(target_dates):
                await asyncio.sleep(2)

    log.info(f"전체 수집 완료: {len(all_rows)}행")
    return all_rows

def parse_ship_row(ship: dict, collected_at: datetime):
    """API 응답 dict → DB row tuple 변환"""
    try:
        fish_types      = ", ".join([f["name"] for f in ship.get("fish_type", [])])
        fishing_methods = ", ".join([m["name"] for m in ship.get("fishing_method", [])])
        price           = ship.get("price", 0)
        price_str       = f"{price:,}원" if price else "전화문의"
        fishing_date_str = ship.get("sdate", "")
        try:
            fishing_date = date.fromisoformat(fishing_date_str) if fishing_date_str else None
        except ValueError:
            fishing_date = None
        return (
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
        )
    except Exception as e:
        log.debug(f"row 파싱 오류: {e} / {ship.get('name','?')}")
        return None

# ── 중복 제거 ────────────────────────────────────────────────
def deduplicate_rows(rows):
    seen = {}
    for row in rows:
        key = (row[1], row[4], row[5], row[15])
        seen[key] = row
    return list(seen.values())

# ── DB 저장 ─────────────────────────────────────────────────
def save_to_postgres(rows, conn, note=""):
    if not rows:
        log.warning("저장할 데이터 없음")
        return 0

    orig = len(rows)
    rows = deduplicate_rows(rows)
    cur  = conn.cursor()
    try:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            ph = ",".join(["(" + ",".join(["%s"] * COLS) + ")"] * len(batch))
            cur.execute(
                f"""
                INSERT INTO fishing_seat_status (
                    collected_at, ship_name, area_name, port_name, fishing_date,
                    depart_time, return_time, fish_types, fishing_methods, price,
                    total_seats, reserved_seats, remain_seats,
                    status_code, status_name, schedule_no
                ) VALUES {ph}
                ON CONFLICT (ship_name, fishing_date, depart_time, schedule_no)
                DO UPDATE SET
                    collected_at    = EXCLUDED.collected_at,
                    remain_seats    = EXCLUDED.remain_seats,
                    reserved_seats  = EXCLUDED.reserved_seats,
                    status_code     = EXCLUDED.status_code,
                    status_name     = EXCLUDED.status_name,
                    area_name       = EXCLUDED.area_name,
                    port_name       = EXCLUDED.port_name,
                    return_time     = EXCLUDED.return_time,
                    fish_types      = EXCLUDED.fish_types,
                    fishing_methods = EXCLUDED.fishing_methods,
                    price           = EXCLUDED.price,
                    total_seats     = EXCLUDED.total_seats
                """,
                [v for row in batch for v in row],
            )
            log.info(f"  배치 저장: {i+len(batch)}/{len(rows)}행")

        cur.execute(
            "INSERT INTO fishing_collect_history (collected_at, row_count, status, note) VALUES (NOW(),%s,%s,%s)",
            (len(rows), "성공", note or f"2026 전체: {orig}행→중복제거{len(rows)}행"),
        )
        conn.commit()
        log.info(f"DB UPSERT 완료: {len(rows)}행")
        return len(rows)

    except Exception as e:
        conn.rollback()
        log.error(f"DB 저장 실패: {e}")
        cur2 = conn.cursor()
        try:
            cur2.execute(
                "INSERT INTO fishing_collect_history (collected_at, row_count, status, note) VALUES (NOW(),%s,%s,%s)",
                (0, "실패", str(e)[:200]),
            )
            conn.commit()
        finally:
            cur2.close()
        raise
    finally:
        cur.close()

# ── JSON 내보내기 ────────────────────────────────────────────
def save_to_json(rows, output_path):
    def _ser(v):
        return v.isoformat() if hasattr(v, "isoformat") else v

    payload = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "row_count": len(rows),
        "rows": [[_ser(v) for v in row] for row in rows],
    }
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    log.info(f"JSON 저장: {output_path} ({len(rows)}행)")

# ── 진입점 ──────────────────────────────────────────────────
def main():
    log.info("=" * 65)
    log.info("어바웃피싱 2026 전체 데이터 수집 시작")
    log.info(f"날짜 범위: {date.today()} ~ 2026-12-31")
    log.info(f"DB: {INSTANCE_CONNECTION_NAME}/{DB_NAME}")
    log.info("=" * 65)
    start = datetime.now()

    try:
        conn = get_db_connection()
        setup_db(conn)
    except Exception as e:
        log.error(f"DB 연결 실패: {e}")
        sys.exit(1)

    # 수집 (에러 나도 계속)
    try:
        rows = asyncio.run(collect_all_2026())
    except Exception as e:
        log.error(f"수집 중 예외 발생: {e}")
        log.error(traceback.format_exc())
        rows = []

    if rows:
        try:
            save_to_postgres(rows, conn, note=f"2026 전체 수집 {len(rows)}행")
        except Exception as e:
            log.error(f"DB 저장 예외: {e}")

        deduped = deduplicate_rows(rows)
        json_path = os.environ.get("JSON_OUTPUT_PATH", "data/latest.json")
        try:
            save_to_json(deduped, json_path)
        except Exception as e:
            log.error(f"JSON 저장 예외: {e}")
    else:
        log.warning("수집된 데이터가 없습니다.")

    if _connector:
        try:
            _connector.close()
        except Exception:
            pass

    elapsed = (datetime.now() - start).total_seconds()
    log.info("=" * 65)
    log.info(f"완료! 소요시간: {elapsed:.1f}초 ({elapsed/60:.1f}분) | 수집행: {len(rows)}")
    log.info("=" * 65)

if __name__ == "__main__":
    main()
