-- ============================================================
-- 어바웃피싱 투자자 대시보드 BigQuery 설정
-- 실행 위치: GCP BigQuery Console (프로젝트: aboutfishingproduct)
-- 주의: fishing.report_metrics_daily 원본 테이블은 수정하지 않음
-- 변경: naver_shopping_ext (외부 테이블) → naver_shopping_data (일반 테이블)
--   Google Sheet 도메인 제한(@aboutfishing.kr only)으로 서비스 계정 접근 불가
--   → Apps Script (yjkim@aboutfishing.kr)가 Sheet 읽어서 BQ에 직접 씀
-- ============================================================

-- ──────────────────────────────────────────────────────────────
-- STEP 1: 네이버 쇼핑 데이터 일반 테이블 생성
-- (Google Apps Script gas_naver_to_bq.js 가 이 테이블에 데이터 적재)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `fishing.naver_shopping_data`
(
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
OPTIONS(
  description = '네이버 쇼핑 광고 일간 데이터 (Apps Script 자동 적재)',
  require_partition_filter = false
);

-- ──────────────────────────────────────────────────────────────
-- STEP 2: 투자자용 Mastered View 생성
-- fishing.report_metrics_daily 원본 + naver_shopping_data 조인
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `fishing.v_investor_mastered` AS
WITH daily AS (
  SELECT
    m.date,
    EXTRACT(YEAR FROM m.date)  AS year,
    EXTRACT(MONTH FROM m.date) AS month,
    FORMAT_DATE('%Y-%m', m.date) AS year_month,

    COALESCE(m.gmv, 0)                  AS gmv_won,
    COALESCE(m.gmv, 0) * 0.03          AS revenue_won,

    COALESCE(m.bookings, 0)            AS bookings,
    COALESCE(m.sessions, 0)            AS sessions,

    COALESCE(
      m.cvr,
      SAFE_DIVIDE(m.bookings * 100.0, NULLIF(m.sessions, 0))
    )                                   AS cvr_pct,

    COALESCE(m.ships, 0)               AS ships,
    COALESCE(m.members, 0)             AS members,

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
  FROM `fishing.naver_shopping_data`
)
SELECT
  d.*,
  COALESCE(n.naver_impressions, 0)     AS naver_impressions,
  COALESCE(n.naver_clicks,      0)     AS naver_clicks,
  COALESCE(n.naver_conversions, 0)     AS naver_conversions,
  COALESCE(n.naver_spend,       0)     AS naver_spend,
  COALESCE(n.naver_revenue,     0)     AS naver_revenue,
  COALESCE(n.naver_roas,        0)     AS naver_roas,
  COALESCE(n.naver_ctr,         0)     AS naver_ctr,
  COALESCE(n.naver_cvr,         0)     AS naver_cvr,

  (COALESCE(d.ad_spend_meta,   0)
   + COALESCE(d.ad_spend_google, 0)
   + COALESCE(n.naver_spend,    0))    AS total_ad_spend,

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

  SUM(gmv_won)                        AS gmv_monthly_won,
  ROUND(SUM(gmv_won) / 10000, 1)      AS gmv_monthly_man,
  SUM(revenue_won)                    AS revenue_monthly_won,

  SUM(bookings)                       AS bookings_monthly,
  SUM(sessions)                       AS sessions_monthly,
  ROUND(AVG(cvr_pct), 2)             AS cvr_avg_pct,

  MAX(ships)                          AS ships_eom,
  MAX(members)                        AS members_eom,

  SUM(ad_spend_meta)                  AS meta_spend_monthly,
  SUM(ad_spend_google)                AS google_spend_monthly,
  SUM(naver_spend)                    AS naver_spend_monthly,
  SUM(total_ad_spend)                 AS total_ad_spend_monthly,

  SUM(naver_impressions)              AS naver_impressions_monthly,
  SUM(naver_clicks)                   AS naver_clicks_monthly,
  SUM(naver_conversions)              AS naver_conversions_monthly,
  ROUND(AVG(NULLIF(naver_roas, 0)), 1) AS naver_roas_avg,

  SAFE_DIVIDE(SUM(gmv_won), SUM(bookings))   AS avg_order_value,
  SAFE_DIVIDE(SUM(total_ad_spend), SUM(bookings)) AS cac_blended

FROM `fishing.v_investor_mastered`
GROUP BY year_month, year, month
ORDER BY year_month DESC;


-- ──────────────────────────────────────────────────────────────
-- STEP 4: 최신 일간 KPI 스냅샷 뷰
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
