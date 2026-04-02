-- ============================================================
-- 어바웃피싱 투자자 대시보드 BigQuery 설정
-- 실행 위치: GCP BigQuery Console (프로젝트: aboutfishingproduct)
-- 주의: fishing.report_metrics_daily 원본 테이블은 수정하지 않음
-- ============================================================

-- ──────────────────────────────────────────────────────────────
-- STEP 1: 네이버 쇼핑 Google Sheets 외부 테이블 생성
-- 사전 조건: BigQuery 서비스 계정에 해당 시트 뷰어 권한 부여 필요
--   (BigQuery 서비스 계정 이메일을 시트에서 공유)
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE EXTERNAL TABLE `fishing.naver_shopping_ext`
(
  -- ※ 아래 컬럼명은 시트 헤더 행 기준 (gid=80217310 탭)
  -- 실제 시트 컬럼명에 맞게 수정하세요.
  report_date    DATE    OPTIONS(description='보고 날짜 (YYYY-MM-DD)'),
  impressions    INT64   OPTIONS(description='노출수'),
  clicks         INT64   OPTIONS(description='클릭수'),
  conversions    INT64   OPTIONS(description='전환수'),
  ad_spend       FLOAT64 OPTIONS(description='광고비 (원)'),
  revenue        FLOAT64 OPTIONS(description='광고 기여 매출 (원)'),
  roas           FLOAT64 OPTIONS(description='ROAS (%)'),
  ctr            FLOAT64 OPTIONS(description='클릭률 (%)'),
  cvr_ad         FLOAT64 OPTIONS(description='광고 전환율 (%)')
)
OPTIONS (
  format             = 'GOOGLE_SHEETS',
  -- gid=80217310 탭을 직접 URI에 포함 (range 예약어 충돌 우회)
  uris               = ['https://docs.google.com/spreadsheets/d/1AGMVHBMBqNrC-HvLHz7iBAp_wk8kAGSgyPIztD-e-AM/edit?range=A:I#gid=80217310'],
  skip_leading_rows  = 1
);

-- ──────────────────────────────────────────────────────────────
-- STEP 2: 투자자용 Mastered View 생성
-- fishing.report_metrics_daily 원본 + 네이버 쇼핑 외부 테이블 조인
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `fishing.v_investor_mastered` AS
WITH daily AS (
  SELECT
    -- 기본 날짜 및 거래 지표
    m.date,
    EXTRACT(YEAR FROM m.date)  AS year,
    EXTRACT(MONTH FROM m.date) AS month,
    FORMAT_DATE('%Y-%m', m.date) AS year_month,

    -- GMV 및 매출 (만원 단위 → 원 단위로 통일. 실제 컬럼 타입 확인 필요)
    -- ※ gmv 컬럼이 이미 원 단위라면 아래 변환식 제거
    COALESCE(m.gmv, 0)                  AS gmv_won,         -- 일간 GMV (원)
    COALESCE(m.gmv, 0) * 0.03          AS revenue_won,      -- 수수료 매출 (GMV × 3%)

    -- 예약 지표
    COALESCE(m.bookings, 0)            AS bookings,
    COALESCE(m.sessions, 0)            AS sessions,

    -- 전환율 (%) — 테이블에 없으면 직접 계산
    COALESCE(
      m.cvr,
      SAFE_DIVIDE(m.bookings * 100.0, NULLIF(m.sessions, 0))
    )                                   AS cvr_pct,

    -- 공급 및 회원 지표 (누적값)
    COALESCE(m.ships, 0)               AS ships,
    COALESCE(m.members, 0)             AS members,

    -- 광고 지표 (테이블에 있는 경우)
    COALESCE(m.ad_spend_meta,    0)    AS ad_spend_meta,
    COALESCE(m.ad_spend_google,  0)    AS ad_spend_google,
    COALESCE(m.ad_roas,          0)    AS ad_roas,
    COALESCE(m.ad_cpa,           0)    AS ad_cpa

  FROM `fishing.report_metrics_daily` m
),
naver AS (
  SELECT
    report_date                         AS date,
    COALESCE(impressions,   0)         AS naver_impressions,
    COALESCE(clicks,        0)         AS naver_clicks,
    COALESCE(conversions,   0)         AS naver_conversions,
    COALESCE(ad_spend,      0)         AS naver_spend,
    COALESCE(revenue,       0)         AS naver_revenue,
    COALESCE(roas,          0)         AS naver_roas,
    COALESCE(ctr,           0)         AS naver_ctr,
    COALESCE(cvr_ad,        0)         AS naver_cvr
  FROM `fishing.naver_shopping_ext`
)
SELECT
  d.*,
  -- 네이버 쇼핑 데이터 (날짜 매칭, 없으면 0)
  COALESCE(n.naver_impressions, 0)     AS naver_impressions,
  COALESCE(n.naver_clicks,      0)     AS naver_clicks,
  COALESCE(n.naver_conversions, 0)     AS naver_conversions,
  COALESCE(n.naver_spend,       0)     AS naver_spend,
  COALESCE(n.naver_revenue,     0)     AS naver_revenue,
  COALESCE(n.naver_roas,        0)     AS naver_roas,
  COALESCE(n.naver_ctr,         0)     AS naver_ctr,
  COALESCE(n.naver_cvr,         0)     AS naver_cvr,

  -- 총 광고비 (Meta + Google + Naver)
  (COALESCE(d.ad_spend_meta,   0)
   + COALESCE(d.ad_spend_google, 0)
   + COALESCE(n.naver_spend,    0))    AS total_ad_spend,

  -- 채널별 광고 기여 예약 비중 (rough estimate용)
  SAFE_DIVIDE(n.naver_conversions,
    NULLIF(d.bookings, 0)) * 100       AS naver_booking_share_pct

FROM daily d
LEFT JOIN naver n ON d.date = n.date
ORDER BY d.date DESC;


-- ──────────────────────────────────────────────────────────────
-- STEP 3: 투자자 대시보드용 월간 집계 뷰
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `fishing.v_investor_monthly` AS
SELECT
  year_month,
  year,
  month,

  -- GMV & 매출
  SUM(gmv_won)                        AS gmv_monthly_won,
  ROUND(SUM(gmv_won) / 10000, 1)      AS gmv_monthly_man,     -- 만원 단위
  SUM(revenue_won)                    AS revenue_monthly_won,

  -- 예약
  SUM(bookings)                       AS bookings_monthly,
  SUM(sessions)                       AS sessions_monthly,
  ROUND(AVG(cvr_pct), 2)             AS cvr_avg_pct,

  -- 공급·회원 (월말 최신값)
  MAX(ships)                          AS ships_eom,
  MAX(members)                        AS members_eom,

  -- 광고비 합계
  SUM(ad_spend_meta)                  AS meta_spend_monthly,
  SUM(ad_spend_google)                AS google_spend_monthly,
  SUM(naver_spend)                    AS naver_spend_monthly,
  SUM(total_ad_spend)                 AS total_ad_spend_monthly,

  -- 네이버 쇼핑 합계
  SUM(naver_impressions)              AS naver_impressions_monthly,
  SUM(naver_clicks)                   AS naver_clicks_monthly,
  SUM(naver_conversions)              AS naver_conversions_monthly,
  ROUND(AVG(NULLIF(naver_roas, 0)), 1) AS naver_roas_avg,

  -- 파생 지표
  SAFE_DIVIDE(SUM(gmv_won), SUM(bookings))   AS avg_order_value,
  SAFE_DIVIDE(SUM(total_ad_spend), SUM(bookings)) AS cac_blended

FROM `fishing.v_investor_mastered`
GROUP BY year_month, year, month
ORDER BY year_month DESC;


-- ──────────────────────────────────────────────────────────────
-- STEP 4: 최신 일간 KPI 스냅샷 뷰 (today / yesterday)
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `fishing.v_investor_kpi_latest` AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
  FROM `fishing.v_investor_mastered`
  WHERE date <= CURRENT_DATE('Asia/Seoul')
)
SELECT
  r.date                              AS report_date,
  r.gmv_won,
  ROUND(r.gmv_won / 10000, 1)        AS gmv_man,
  r.revenue_won,
  r.bookings,
  r.sessions,
  r.cvr_pct,
  r.ships,
  r.members,
  r.total_ad_spend,
  r.naver_spend,
  r.ad_spend_meta,
  r.ad_spend_google,

  -- WoW 변화율 (7일 전 대비)
  SAFE_DIVIDE(
    r.gmv_won - prev.gmv_won, NULLIF(prev.gmv_won, 0)
  ) * 100                            AS gmv_wow_pct,

  SAFE_DIVIDE(
    r.bookings - prev.bookings, NULLIF(prev.bookings, 0)
  ) * 100                            AS bookings_wow_pct

FROM ranked r
LEFT JOIN (
  SELECT date, gmv_won, bookings
  FROM `fishing.v_investor_mastered`
) prev ON prev.date = DATE_SUB(r.date, INTERVAL 7 DAY)
WHERE r.rn = 1;
