#!/usr/bin/env python3
"""
통합 좌석 수집 스크립트 v2
- 선상24 (api.sunsang24.com) + 더피싱 (thefishing.kr) 병렬 수집
- AF boat_id 매핑 (선박명+항구 기반)
- PostgreSQL 저장 + GCS seat-data/YYYY-MM-DD.json 업로드
- 오버부킹 방지: 실시간 남은 좌석 기준

실행: python3 collect_unified.py
환경변수:
  INSTANCE_CONNECTION_NAME, DB_NAME, DB_USER, DB_PASSWORD → PostgreSQL
  GOOGLE_APPLICATION_CREDENTIALS → GCS
"""

import asyncio
import json
import logging
import os
import re
import subprocess
import shutil
from datetime import date, datetime, timedelta
from typing import Optional

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── 설정 ──────────────────────────────────────────────────────────────────
GCS_BUCKET    = "booking-redirect-aboutfishing-kr"
GCS_PREFIX    = "seat-data"
DAYS_AHEAD    = int(os.environ.get("DAYS_AHEAD", "14"))   # 오늘 포함 N일치 (기본 14일)
SKIP_EXISTING = os.environ.get("SKIP_EXISTING", "").lower() in ("1", "true", "yes")  # 기존 GCS 날짜 스킵
MAX_CONC      = 10

# sunsang24 API
SS24_API     = "https://api.sunsang24.com/ship/list"
SS24_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept":     "application/json, text/plain, */*",
    "Referer":    "https://www.sunsang24.com/",
    "Origin":     "https://www.sunsang24.com",
    "platform":   "web",
}

# 더피싱 API
TF_AJAX_URL  = "https://thefishing.kr/reservation/list.view.view.1.ajax.new.php"
TF_LIST_URL  = "https://thefishing.kr/reservation/list.php"
TF_HEADERS   = {
    "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Content-Type":    "application/x-www-form-urlencoded; charset=utf-8",
    "Referer":         "https://thefishing.kr/reservation/list.php",
    "X-Requested-With":"XMLHttpRequest",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

# ── AF boat_id 매핑 테이블 ─────────────────────────────────────────────────
# KEY: (sunsang24_ship_name, area_sub)  →  af_boat_id
AF_MAPPING = {
    ("K세븐호",        "중구"):          1717,
    ("검은모래호",     "여수"):          2166,
    ("경진호",         "고성"):          1992,
    ("경진호",         "사천"):          2108,
    ("골드로저호",     "여수"):          1773,
    ("김대영호",       "군산"):          2238,
    ("나라호",         "중구"):          2279,
    ("나래호",         "창원"):          1961,
    ("나폴레옹호",     "태안"):          451,
    ("노을바다호",     "당진"):          784,
    ("뉴고니호",       "창원"):          2090,
    ("뉴벤쿠버호",     "거제"):          961,
    ("뉴스타호",       "태안"):          2288,
    ("뉴스타호",       "고성"):          1998,
    ("뉴해경호",       "창원"):          2253,
    ("뉴해양호",       "서천"):          1990,
    ("대성호",         "통영"):          1756,
    ("대성호",         "태안"):          2246,
    ("더블케이2호",    "사천"):          1870,
    ("더블케이호",     "사천"):          1867,
    ("도리스피싱",     "사천"):          1496,
    ("돌핀호",         "고성"):          2375,
    ("돌핀호",         "시흥"):          1419,
    ("돌핀호",         "평택"):          2027,
    ("동백호",         "홍성"):          993,
    ("동백호",         "태안"):          1558,
    ("동인호",         "포항"):          2422,
    ("드래곤호",       "삼척"):          1256,
    ("드래곤호",       "여수"):          1093,
    ("드래곤호",       "군산"):          487,
    ("라임호",         "통영"):          2069,
    ("레드호",         "태안"):          342,
    ("로아2호",        "고흥"):          1823,
    ("로아호",         "고흥"):          1822,
    ("로이스호",       "통영"):          2301,
    ("로즈마리호",     "서귀포"):        1597,
    ("만선호",         "태안"):          64,
    ("몬스터호",       "고흥"):          732,
    ("몬스터호",       "여수"):          1896,
    ("미르호",         "중구"):          1405,
    ("미르호",         "고성"):          1826,
    ("바다사랑호",     "태안"):          1373,
    ("베테랑호",       "보령"):          825,
    ("베테랑호",       "포항"):          2218,
    ("블랙이글스3호",  "고성"):          813,
    ("블랙이글스호",   "고성"):          353,
    ("블랙이글스호",   "태안"):          1655,
    ("비엔나호",       "보령"):          2072,
    ("빅토리호",       "고흥"):          1138,
    ("빅토리호",       "태안"):          338,
    ("빨강등대호",     "시흥"):          1202,
    ("서린호",         "부산"):          2025,
    ("세븐호",         "중구"):          1980,
    ("수호2호",        "고흥"):          1899,
    ("신가피싱",       "사천"):          1334,
    ("씨스타호",       "홍성"):          1422,
    ("씨스타호",       "경주"):          1357,
    ("씨스타호",       "제주"):          2012,
    ("씨헌터호",       "서천"):          2001,
    ("아시아호",       "군산"):          155,
    ("아쿠아호",       "창원"):          1765,
    ("안흥2호",        "태안"):          2028,
    ("알파호",         "통영"):          2206,
    ("에스코트호",     "경주"):          1201,
    ("에이스피싱호",   "평택"):          1683,
    ("에이스호",       "보령"):          369,
    ("에이스호",       "군산"):          1701,
    ("에이스호",       "제주"):          1991,
    ("에이스호",       "여수"):          2286,
    ("에이스호",       "울진"):          2344,
    ("에프원호",       "중구"):          164,
    ("야호",           "여수"):          2098,
    ("와일드캣호",     "태안"):          2118,
    ("용왕호",         "군산"):          1999,
    ("우정호",         "강릉"):          2339,
    ("일광호",         "고성"):          1233,
    ("일출호",         "제주"):          2079,
    ("자유호",         "태안"):          2244,
    ("짱구호",         "창원"):          2101,
    ("청운호",         "태안"):          2037,
    ("코리아호",       "고성"):          1993,
    ("킹덤호",         "통영"):          1430,
    ("킹몬스터호",     "여수"):          1414,
    ("킹콩2호",        "중구"):          69,
    ("트라이앵글호",   "보령"):          1547,
    ("포세이돈호",     "강릉"):          1621,
    ("포세이돈호",     "포항"):          2193,
    ("포세이돈호",     "군산"):          2372,
    ("포세이돈호",     "시흥"):          831,
    ("포세이돈호",     "제주"):          1068,
    ("포인트호",       "사천"):          1334,
    ("하모니호",       "태안"):          193,
    ("해광호",         "보령"):          2113,
    ("해밀호",         "군산"):          2094,
    ("해모수호",       "보령"):          484,
    ("해모수호",       "사천"):          1216,
    ("해운호",         "거제"):          1518,
    ("현대호",         "태안"):          2223,
    ("현대호",         "강릉"):          1664,
    ("홀리호",         "강릉"):          1668,
    ("화인호",         "중구"):          1145,
    ("황금어장호",     "태안"):          2074,
    ("황금어장호",     "보령"):          2074,
    ("싹쓰리호",       "강릉"):          1198,
    ("싹쓰리호",       "포항"):          1376,
    ("성용호",         "통영"):          2205,
}

# af_boat_id → (lat, lng)
AF_COORDS = {
    64:   (36.7456, 126.2979),
    69:   (37.4562, 126.6086),
    155:  (35.9833, 126.5667),
    164:  (37.4705, 126.6163),
    193:  (36.7456, 126.2979),
    338:  (36.7456, 126.2979),
    342:  (36.7456, 126.2979),
    353:  (38.3833, 128.4667),
    369:  (36.3494, 126.5081),
    451:  (36.7456, 126.2979),
    484:  (36.3494, 126.5081),
    487:  (35.9833, 126.5667),
    731:  (34.6141, 127.2868),
    732:  (34.6141, 127.2868),
    784:  (36.9894, 127.0894),
    813:  (38.3833, 128.4667),
    825:  (36.3494, 126.5081),
    831:  (37.3817, 126.8028),
    961:  (34.8783, 128.6219),
    993:  (36.5758, 126.6591),
    1068: (33.5172, 126.5218),
    1093: (34.7604, 127.6622),
    1138: (34.6141, 127.2868),
    1145: (37.4705, 126.6163),
    1198: (37.7833, 128.9167),
    1201: (35.8562, 129.2245),
    1202: (37.3817, 126.8028),
    1216: (34.9249, 128.0771),
    1233: (38.3833, 128.4667),
    1246: (38.3833, 128.4667),
    1256: (37.4500, 129.1667),
    1334: (34.9249, 128.0771),
    1357: (35.8562, 129.2245),
    1373: (36.7456, 126.2979),
    1376: (36.0167, 129.3833),
    1405: (37.4705, 126.6163),
    1414: (34.7604, 127.6622),
    1419: (37.3817, 126.8028),
    1422: (36.5758, 126.6591),
    1430: (34.8544, 128.4330),
    1496: (34.9249, 128.0771),
    1518: (34.8783, 128.6219),
    1547: (36.3494, 126.5081),
    1558: (36.7456, 126.2979),
    1597: (33.2489, 126.5678),
    1621: (37.7833, 128.9167),
    1655: (36.7456, 126.2979),
    1664: (37.7833, 128.9167),
    1668: (37.7833, 128.9167),
    1683: (36.9894, 127.0894),
    1701: (35.9833, 126.5667),
    1717: (37.4562, 126.6086),
    1756: (34.8544, 128.4330),
    1765: (35.1425, 128.5542),
    1773: (34.7604, 127.6622),
    1822: (34.6141, 127.2868),
    1823: (34.6141, 127.2868),
    1826: (38.3833, 128.4667),
    1867: (34.9249, 128.0771),
    1870: (34.9249, 128.0771),
    1896: (34.7604, 127.6622),
    1899: (34.6141, 127.2868),
    1961: (35.1425, 128.5542),
    1980: (37.4705, 126.6163),
    1990: (36.2294, 126.5875),
    1991: (33.5172, 126.5218),
    1992: (38.3833, 128.4667),
    1993: (38.3833, 128.4667),
    1998: (38.3833, 128.4667),
    1999: (35.9833, 126.5667),
    2001: (36.2294, 126.5875),
    2012: (33.5172, 126.5218),
    2025: (35.1000, 129.0444),
    2027: (36.9894, 127.0894),
    2028: (36.7456, 126.2979),
    2037: (36.7456, 126.2979),
    2069: (34.8544, 128.4330),
    2072: (36.3494, 126.5081),
    2074: (36.7456, 126.2979),
    2079: (33.5172, 126.5218),
    2090: (35.1944, 128.5806),
    2094: (35.9833, 126.5667),
    2098: (34.7604, 127.6622),
    2101: (35.1944, 128.5806),
    2108: (34.9249, 128.0771),
    2113: (36.3494, 126.5081),
    2118: (36.7456, 126.2979),
    2166: (34.7604, 127.6622),
    2193: (36.0167, 129.3833),
    2205: (34.8544, 128.4330),
    2206: (34.8544, 128.4330),
    2218: (36.0167, 129.3833),
    2223: (36.7456, 126.2979),
    2238: (35.9833, 126.5667),
    2244: (36.7456, 126.2979),
    2246: (36.7456, 126.2979),
    2253: (35.1425, 128.5542),
    2279: (37.4562, 126.6086),
    2286: (34.7604, 127.6622),
    2288: (36.7456, 126.2979),
    2301: (34.8544, 128.4330),
    2339: (37.7833, 128.9167),
    2344: (37.0000, 129.4000),
    2372: (35.9833, 126.5667),
    2375: (38.3833, 128.4667),
    2422: (36.0167, 129.3833),
}

# 더피싱 uid → af_boat_id 매핑
# uid: (pa_uid, st_uid, af_boat_id, ship_name)
TF_MAPPING = {
    "3515": (3515, 141,  825,  "예린호"),        # 보령 예린호
    "252":  (252,  48,   64,   "만선호"),         # 태안 만선호
    "248":  (248,  47,   342,  "레드호"),         # 태안 레드호
    "543":  (543,  97,   338,  "빅토리호"),       # 태안 빅토리호
    "1295": (1295, 209,  451,  "나폴레옹호"),     # 태안 나폴레옹호
    "2840": (2840, 352,  155,  "서해피싱호"),     # 군산
    "3487": (3487, 436,  2244, "자유호"),         # 태안
    "3941": (3941, 493,  2118, "와일드캣호"),     # 태안
    "4644": (4644, 568,  2074, "황금어장호"),     # 태안
    "4935": (4935, 764,  None, "포인트피싱"),     # 미매핑
}

# 이름 기반 AF 매핑 역인덱스
_AF_BY_NAME: dict[str, list] = {}
for (name, area), bid in AF_MAPPING.items():
    _AF_BY_NAME.setdefault(name, []).append((area, bid))


def get_af_boat_id(ship_name: str, area_sub: str) -> Optional[int]:
    """선박명+지역으로 AF boat_id 반환 (정확→부분→이름전용→fuzzy)"""
    key = (ship_name, area_sub)
    if key in AF_MAPPING:
        return AF_MAPPING[key]
    # 지역 부분 일치
    for (name, area), bid in AF_MAPPING.items():
        if name == ship_name and (area in area_sub or area_sub in area):
            return bid
    # 이름만으로 유일
    entries = _AF_BY_NAME.get(ship_name, [])
    if len(entries) == 1:
        return entries[0][1]
    # fuzzy: 포함 관계
    for af_name, elist in _AF_BY_NAME.items():
        if af_name in ship_name or ship_name in af_name:
            if len(elist) == 1:
                return elist[0][1]
    return None


def area_to_main(area_sub: str) -> str:
    """area_sub → area_main 변환"""
    mapping = {
        "중구": "인천", "시흥": "경기", "안산": "경기", "화성": "경기", "평택": "경기",
        "태안": "충남", "보령": "충남", "홍성": "충남", "당진": "충남", "서천": "충남",
        "군산": "전북", "부안": "전북",
        "여수": "전남", "고흥": "전남", "무안": "전남",
        "통영": "경남", "사천": "경남", "거제": "경남", "창원": "경남", "고성": "경남",
        "강릉": "강원", "동해": "강원", "삼척": "강원", "속초": "강원",
        "제주": "제주", "서귀포": "제주",
        "포항": "경북", "울진": "경북", "경주": "경북",
        "부산": "부산", "울산": "울산",
        "완도": "전남", "신안": "전남",
    }
    for sub, main in mapping.items():
        if sub in area_sub:
            return main
    return "기타"


# ── 선상24 수집 ────────────────────────────────────────────────────────────
async def fetch_ss24_page(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                           sdate: str, page: int) -> tuple[int, list]:
    """선상24 API 한 페이지 수집, (total, items) 반환"""
    params = {"page": page, "type": "general", "sdate": sdate}
    async with sem:
        try:
            async with session.get(SS24_API, params=params,
                                   timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status != 200:
                    return 0, []
                d = await resp.json(content_type=None)
                return d.get("total", 0), d.get("list", [])
        except Exception as e:
            log.warning(f"[SS24] page={page} date={sdate} 에러: {e}")
            return 0, []


async def collect_ss24_date(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                             sdate: str) -> list:
    """특정 날짜 선상24 전체 수집"""
    total, first_items = await fetch_ss24_page(session, sem, sdate, 1)
    if not total:
        return []
    import math
    pages = math.ceil(total / 30)
    log.info(f"  [SS24 {sdate}] total={total}, pages={pages}")

    all_items = list(first_items)
    if pages > 1:
        tasks = [fetch_ss24_page(session, sem, sdate, p) for p in range(2, pages + 1)]
        results = await asyncio.gather(*tasks)
        for _, items in results:
            all_items.extend(items)
    return all_items


def parse_ss24_item(item: dict, source_date: str) -> Optional[dict]:
    """선상24 API item → ships[] 포맷"""
    ship = item.get("ship", {})
    ship_no_ss24 = ship.get("no")
    ship_name    = (ship.get("name") or "").strip()
    area_sub     = (ship.get("area_sub") or "").strip()
    area_main    = ship.get("area_main") or area_to_main(area_sub)
    lat          = ship.get("lat")
    lng          = ship.get("lng")
    sdate        = item.get("sdate", source_date)
    stime        = (item.get("stime") or "")[:5]
    etime        = (item.get("etime") or "")[:5]
    fish_type    = item.get("fish_type", "")
    method       = item.get("fishing_method", "")
    remain       = item.get("remain_embarkation_num") or 0
    status_code  = item.get("schedule_status_code") or "ING"
    status_name  = item.get("schedule_status_name") or ""
    price_raw    = item.get("price")
    schedule_no  = item.get("schedule_no")

    try:
        price = int(price_raw) if price_raw else None
    except (ValueError, TypeError):
        price = None

    af_boat_id = get_af_boat_id(ship_name, area_sub)
    # AF 좌표 우선, sunsang24 좌표 fallback
    if af_boat_id and af_boat_id in AF_COORDS:
        lat, lng = AF_COORDS[af_boat_id]

    return {
        "ship_no":        af_boat_id,
        "ss24_ship_no":   ship_no_ss24,
        "ship_name":      ship_name,
        "area_main":      area_main,
        "area_sub":       area_sub,
        "lat":            lat,
        "lng":            lng,
        "sdate":          sdate,
        "stime":          stime,
        "etime":          etime,
        "fish_type":      fish_type,
        "method":         method,
        "remain":         remain,
        "status":         status_code,
        "status_name":    status_name,
        "price":          price,
        "schedule_no":    schedule_no,
        "source":         "sunsang24",
    }


# ── 더피싱 수집 ────────────────────────────────────────────────────────────
async def fetch_tf_ship_list(session: aiohttp.ClientSession) -> list[dict]:
    """더피싱 전체 선박 목록 수집 (pa_uid, st_uid, name)"""
    import re
    ships = []
    try:
        async with session.get(TF_LIST_URL, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            html = await resp.text()
        # uid 링크
        uids = re.findall(r'href=["\'].*?uid=(\d+)["\']', html)
        uids = list(dict.fromkeys(uids))

        # 각 uid에서 st_uid 추출
        for pa_uid in uids[:60]:
            async with session.get(f"{TF_LIST_URL}?uid={pa_uid}",
                                   timeout=aiohttp.ClientTimeout(total=15)) as rp:
                page_html = await rp.text()
            # 선박명
            name_m = re.search(r'<title>(?:\[.+?\])?\s*(.+?)\s*[-|]', page_html)
            ship_name = name_m.group(1).strip() if name_m else ""
            # st_uid
            st_m = re.search(r"st_uid=(\d+)&pa_uid=" + pa_uid, page_html)
            if not st_m:
                st_m = re.search(r"'(\d+)'[,\s]*pa_uid.*?" + pa_uid, page_html)
            if not st_m:
                st_m = re.search(r"type1_ajax.*?st_uid=(\d+)", page_html)
            st_uid = st_m.group(1) if st_m else None
            # area/port 파악
            area_m = re.search(r'\[([가-힣]+항?)\s+([가-힣]+호?)\]', page_html)
            area_sub = area_m.group(1).rstrip('항').strip() if area_m else ""

            if st_uid:
                ships.append({
                    "pa_uid":    pa_uid,
                    "st_uid":    st_uid,
                    "ship_name": ship_name,
                    "area_sub":  area_sub,
                })
        log.info(f"  [TF] 선박 목록 수집: {len(ships)}척")
    except Exception as e:
        log.warning(f"[TF] 선박 목록 수집 실패: {e}")
    return ships


async def fetch_tf_seats(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                          pa_uid: str, st_uid: str,
                          year: int, month: int) -> list[dict]:
    """더피싱 특정 선박 월별 좌석 데이터"""
    import re
    from bs4 import BeautifulSoup

    date6 = f"{year}{month:02d}"
    data  = f"date6={date6}&st_uid={st_uid}&pa_uid={pa_uid}"
    results = []
    async with sem:
        try:
            async with session.post(TF_AJAX_URL, data=data,
                                    timeout=aiohttp.ClientTimeout(total=20)) as resp:
                html = await resp.text()
        except Exception as e:
            log.warning(f"[TF] uid={pa_uid} {date6} 에러: {e}")
            return []

    soup = BeautifulSoup(html, "html.parser")
    # 각 날짜 셀: <div class="dayline"><span class="day">1</span>...
    for td in soup.find_all("td"):
        day_el  = td.find("span", class_="day")
        num_el  = td.find("span", class_="num")
        if not (day_el and num_el):
            continue
        day = day_el.get_text(strip=True)
        if not day.isdigit():
            continue
        # 완전 날짜
        full_date = f"{year}-{month:02d}-{int(day):02d}"
        # 남은 인원
        try:
            remain = int(num_el.get_text(strip=True))
        except ValueError:
            remain = 0
        # 어종/가격
        fish_el  = td.find("ul", class_="schedule2")
        fish_str = fish_el.get_text(" ", strip=True) if fish_el else ""
        # 예약 링크에서 날짜 검증
        link_el = td.find("a", onclick=re.compile(r"date=\d{8}"))
        if link_el:
            dm = re.search(r"date=(\d{8})", link_el.get("onclick", ""))
            if dm:
                full_date = f"{dm.group(1)[:4]}-{dm.group(1)[4:6]}-{dm.group(1)[6:]}"

        results.append({
            "sdate":   full_date,
            "remain":  remain,
            "fish_str": fish_str[:100],
            "status":  "ING" if remain > 0 else "FULL",
        })
    return results


# ── ships[] 포맷 변환 ──────────────────────────────────────────────────────
def tf_to_ships(tf_ship: dict, seat_list: list, af_boat_id: Optional[int]) -> list[dict]:
    """더피싱 좌석 데이터 → ships[] 포맷"""
    results = []
    ship_name = tf_ship["ship_name"]
    area_sub  = tf_ship["area_sub"]
    area_main = area_to_main(area_sub)

    lat, lng = None, None
    if af_boat_id and af_boat_id in AF_COORDS:
        lat, lng = AF_COORDS[af_boat_id]

    for seat in seat_list:
        sdate = seat["sdate"]
        # 오늘 이전 날짜 제외
        try:
            if date.fromisoformat(sdate) < date.today():
                continue
        except ValueError:
            continue

        results.append({
            "ship_no":     af_boat_id,
            "tf_pa_uid":   tf_ship["pa_uid"],
            "ship_name":   ship_name,
            "area_main":   area_main,
            "area_sub":    area_sub,
            "lat":         lat,
            "lng":         lng,
            "sdate":       sdate,
            "stime":       "",
            "etime":       "",
            "fish_type":   seat.get("fish_str", ""),
            "method":      "",
            "remain":      seat["remain"],
            "status":      seat["status"],
            "status_name": "예약가능" if seat["remain"] > 0 else "예약마감",
            "price":       None,
            "schedule_no": f"tf_{tf_ship['pa_uid']}_{sdate}",
            "source":      "thefishing",
        })
    return results


# ── GCS 업로드 ─────────────────────────────────────────────────────────────
def upload_to_gcs(local_path: str, gcs_path: str) -> bool:
    gsutil = shutil.which("gsutil") or \
             "/sessions/sweet-adoring-thompson/.gcloud/google-cloud-sdk/bin/gsutil"
    cmd = [
        gsutil,
        "-h", "Cache-Control:no-cache,no-store,max-age=0,must-revalidate",
        "-h", "Content-Type:application/json;charset=utf-8",
        "cp", local_path, f"gs://{GCS_BUCKET}/{gcs_path}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"GCS 업로드 실패 ({gcs_path}): {result.stderr[:200]}")
        return False
    log.info(f"  ✅ GCS 업로드: {gcs_path}")
    return True


def gcs_file_exists(gcs_path: str) -> bool:
    """GCS에 해당 파일이 이미 존재하는지 확인 (단일 파일)"""
    gsutil = shutil.which("gsutil") or "/usr/bin/gsutil"
    result = subprocess.run(
        [gsutil, "stat", f"gs://{GCS_BUCKET}/{gcs_path}"],
        capture_output=True, text=True
    )
    return result.returncode == 0


def get_gcs_existing_dates() -> set:
    """GCS에 이미 존재하는 날짜 목록을 한 번의 ls 호출로 가져옴 (성능 최적화)"""
    gsutil = shutil.which("gsutil") or "/usr/bin/gsutil"
    result = subprocess.run(
        [gsutil, "ls", f"gs://{GCS_BUCKET}/{GCS_PREFIX}/"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        log.warning(f"GCS ls 실패, stat 방식으로 fallback: {result.stderr[:100]}")
        return set()
    existing = set()
    for line in result.stdout.splitlines():
        # gs://bucket/seat-data/2026-04-05.json → 2026-04-05
        fname = line.strip().split("/")[-1]
        if fname.endswith(".json") and len(fname) == 15:  # YYYY-MM-DD.json
            existing.add(fname[:10])
    log.info(f"  GCS 기존 날짜: {len(existing)}일")
    return existing


# ── 메인 ───────────────────────────────────────────────────────────────────
async def main():
    log.info("=" * 65)
    log.info("통합 좌석 수집 v2: 선상24 + 더피싱 → AF 매핑 → GCS")
    log.info(f"수집 기간: 오늘~{DAYS_AHEAD}일 ({date.today()} ~ {date.today()+timedelta(days=DAYS_AHEAD-1)})")
    if SKIP_EXISTING:
        log.info("  ※ SKIP_EXISTING=true: 기존 GCS 날짜는 재수집 생략")
    log.info("=" * 65)
    start = datetime.now()

    # 날짜 범위
    all_dates = [(date.today() + timedelta(days=i)).isoformat() for i in range(DAYS_AHEAD)]

    # SKIP_EXISTING: 이미 GCS에 있는 날짜 필터링 (단일 ls 호출)
    if SKIP_EXISTING:
        existing_dates = get_gcs_existing_dates()
        if existing_dates:
            # ls 성공: 한 번에 필터링
            dates   = [d for d in all_dates if d not in existing_dates]
            skipped = [d for d in all_dates if d in existing_dates]
        else:
            # ls 실패: 개별 stat fallback
            skipped, dates = [], []
            for d in all_dates:
                (skipped if gcs_file_exists(f"{GCS_PREFIX}/{d}.json") else dates).append(d)
        if skipped:
            log.info(f"  GCS 기존 데이터 스킵: {len(skipped)}일 ({skipped[0]}~{skipped[-1]})")
        log.info(f"  신규 수집 대상: {len(dates)}일")
    else:
        dates = all_dates
    if not dates:
        log.info("  ✅ 수집할 신규 날짜 없음 (모두 GCS에 존재). 종료.")
        return

    months = list(dict.fromkeys(
        (int(d[:4]), int(d[5:7])) for d in dates
    ))

    sem = asyncio.Semaphore(MAX_CONC)
    connector = aiohttp.TCPConnector(limit=MAX_CONC, ssl=False)

    # 날짜별 ships 집계
    date_ships: dict[str, list] = {d: [] for d in dates}
    seen_schedules: set = set()

    async with aiohttp.ClientSession(headers=SS24_HEADERS, connector=connector) as ss24_session:
        # ── 선상24 수집 ──
        log.info("\n▶ 선상24 수집 시작...")
        ss24_tasks = [collect_ss24_date(ss24_session, sem, d) for d in dates]
        ss24_results = await asyncio.gather(*ss24_tasks)

        ss24_total = 0
        for d, items in zip(dates, ss24_results):
            for item in items:
                parsed = parse_ss24_item(item, d)
                if not parsed:
                    continue
                sch_key = (parsed["ship_name"], parsed["sdate"], parsed["stime"], parsed.get("schedule_no"))
                if sch_key in seen_schedules:
                    continue
                seen_schedules.add(sch_key)
                date_ships[d].append(parsed)
                ss24_total += 1
        log.info(f"  선상24 수집 완료: {ss24_total}건")

    # ── 더피싱 수집 ──
    log.info("\n▶ 더피싱 수집 시작...")
    tf_connector = aiohttp.TCPConnector(limit=5, ssl=False)
    async with aiohttp.ClientSession(headers=TF_HEADERS, connector=tf_connector) as tf_session:
        # 선박 목록 수집
        tf_ships = await fetch_tf_ship_list(tf_session)

        tf_total = 0
        for tf_ship in tf_ships:
            pa_uid    = tf_ship["pa_uid"]
            st_uid    = tf_ship["st_uid"]
            ship_name = tf_ship["ship_name"]
            area_sub  = tf_ship["area_sub"]
            af_id     = get_af_boat_id(ship_name, area_sub)

            # 필요한 월들만 수집
            month_seats: list[dict] = []
            for year, month in months:
                seats = await fetch_tf_seats(tf_session, sem, pa_uid, st_uid, year, month)
                month_seats.extend(seats)

            ships_rows = tf_to_ships(tf_ship, month_seats, af_id)
            for row in ships_rows:
                d = row["sdate"]
                if d in date_ships:
                    # 중복 체크 (동일 선박+날짜)
                    sch_key = (row["ship_name"], row["sdate"], row.get("stime", ""), row.get("schedule_no"))
                    if sch_key not in seen_schedules:
                        seen_schedules.add(sch_key)
                        date_ships[d].append(row)
                        tf_total += 1

            if af_id:
                log.info(f"  [TF] {ship_name} ({area_sub}) → AF:{af_id} | {len(ships_rows)}건")
            else:
                log.info(f"  [TF] {ship_name} ({area_sub}) → AF:미매핑 | {len(ships_rows)}건")

        log.info(f"  더피싱 수집 완료: {tf_total}건")

    # ── 날짜별 JSON + GCS 업로드 ──
    log.info("\n▶ GCS 업로드 시작...")
    os.makedirs("/tmp/seat-out", exist_ok=True)
    uploaded = []
    index_data = {"dates": [], "updated_at": datetime.utcnow().isoformat() + "Z"}

    for d in dates:
        ships = date_ships[d]
        if not ships:
            log.info(f"  [{d}] 0건, 건너뜀")
            continue

        af_matched = sum(1 for s in ships if s.get("ship_no"))
        payload = {
            "date":       d,
            "scraped_at": datetime.utcnow().isoformat() + "Z",
            "total":      len(ships),
            "af_matched": af_matched,
            "ships":      ships,
        }
        local_path = f"/tmp/seat-out/{d}.json"
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

        gcs_path = f"{GCS_PREFIX}/{d}.json"
        if upload_to_gcs(local_path, gcs_path):
            uploaded.append(d)
            index_data["dates"].append({
                "date": d,
                "total": len(ships),
                "af_matched": af_matched,
            })
        log.info(f"  [{d}] {len(ships)}건 (AF매핑: {af_matched}건)")

    # index.json 업로드
    idx_path = "/tmp/seat-out/index.json"
    with open(idx_path, "w", encoding="utf-8") as f:
        json.dump(index_data, f, ensure_ascii=False, separators=(",", ":"))
    upload_to_gcs(idx_path, f"{GCS_PREFIX}/index.json")

    # ── data/latest.json 작성 (GitHub Pages 대시보드용) ──
    all_ships_flat: list[dict] = []
    for d in dates:
        all_ships_flat.extend(date_ships[d])

    json_out_path = os.environ.get("JSON_OUTPUT_PATH", "data/latest.json")
    write_latest_json(all_ships_flat, json_out_path)

    elapsed = (datetime.now() - start).total_seconds()
    log.info("\n" + "=" * 65)
    log.info(f"✅ 완료! 업로드: {len(uploaded)}일 / {elapsed:.1f}초")
    log.info(f"   선상24: {ss24_total}건 | 더피싱: {tf_total}건")
    log.info(f"   latest.json: {len(all_ships_flat)}건")
    log.info("=" * 65)


def write_latest_json(all_ships: list[dict], json_path: str) -> None:
    """대시보드용 data/latest.json 작성
    rows 포맷: [collected_at, ship_name, area_name, port_name, fishing_date,
                depart_time, return_time, fish_types, fishing_methods, price,
                total_seats, reserved_seats, remain_seats, status_code,
                status_name, schedule_no, af_boat_id, source, lat, lng]
    """
    now_str = datetime.utcnow().isoformat() + "Z"
    rows = []
    for s in all_ships:
        price_str = str(s["price"]) if s.get("price") else ""
        rows.append([
            now_str,                          # 0 collected_at
            s.get("ship_name", ""),            # 1 ship_name
            s.get("area_main", ""),            # 2 area_name
            s.get("area_sub", ""),             # 3 port_name
            s.get("sdate", ""),                # 4 fishing_date
            s.get("stime", ""),                # 5 depart_time
            s.get("etime", ""),                # 6 return_time
            s.get("fish_type", ""),            # 7 fish_types
            s.get("method", ""),               # 8 fishing_methods
            price_str,                         # 9 price
            0,                                 # 10 total_seats (API 미제공)
            0,                                 # 11 reserved_seats (API 미제공)
            s.get("remain", 0),                # 12 remain_seats
            s.get("status", "ING"),            # 13 status_code
            s.get("status_name", ""),          # 14 status_name
            s.get("schedule_no", ""),          # 15 schedule_no
            s.get("ship_no"),                  # 16 af_boat_id
            s.get("source", ""),               # 17 source
            s.get("lat"),                      # 18 lat
            s.get("lng"),                      # 19 lng
        ])

    payload = {
        "updated_at": now_str,
        "row_count":  len(rows),
        "rows":       rows,
    }
    os.makedirs(os.path.dirname(os.path.abspath(json_path)), exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    log.info(f"  ✅ latest.json 저장: {json_path} ({len(rows)}건)")


if __name__ == "__main__":
    asyncio.run(main())
