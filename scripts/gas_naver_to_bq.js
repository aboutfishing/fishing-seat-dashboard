/**
 * 어바웃피싱 — 네이버 쇼핑 광고 데이터 BigQuery 동기화
 * =====================================================
 * 실행 계정: yjkim@aboutfishing.kr (Google Workspace)
 * 실행 방식: Google Apps Script (이 파일을 스크립트 편집기에 붙여넣기)
 *
 * 설정 방법:
 *   1. Google Sheet 열기 (시트 ID: 1AGMVHBMBqNrC-HvLHz7iBAp_wk8kAGSgyPIztD-e-AM)
 *   2. 확장 프로그램 → Apps Script 클릭
 *   3. 이 파일 전체를 복사하여 편집기에 붙여넣기
 *   4. 상단 저장 버튼 클릭
 *   5. syncNaverToBigQuery 함수 실행하여 권한 승인
 *   6. 트리거 설정: 시계 아이콘 → 트리거 추가 → syncNaverToBigQuery → 매일
 *
 * 대상 BigQuery 테이블: aboutfishingproduct.fishing.naver_shopping_data
 */

// ── 설정 ──────────────────────────────────────────────────────────────────────

// ── 시트 구조 확인 결과 (2026-04-06) ───────────────────────────────────────────
// 현재 시트 탭 목록:
//   - Main
//   - 2026 Weekly dashboard(플랫폼 사업팀)
//   - 2026 Monthly dashboard(플랫폼 사업팀)
//   - 2026 플랫폼 보조지표(Week)
//   - 2026 플랫폼 보조지표(Month)
//   - 회원탈퇴_RAW(DB_manual)
//   - 커뮤니티/쇼핑탭 체류시간(DB_manual)  ← GID 80217310 (커뮤니티/쇼핑 페이지뷰)
//   - 주별 총 사용자_RAW(manual)
//   - 주별 활성 사용자_RAW(manual)
//   - 샵바이_퍼널_RAW(manual)
//   - 월별 총 사용자_RAW(manual)
//   - 월별 활성 사용자_RAW(manual)
//
// ⚠️ 네이버 쇼핑 광고(노출수/클릭수/전환수/광고비/ROAS) 탭이 없음
// → 아래 SHEET_TAB_NAME을 실제 네이버 광고 데이터가 있는 탭명으로 수정하거나
//    새 탭을 추가한 뒤 이 스크립트를 실행하세요.
//
// 새 탭 생성 시 권장 헤더 (2행부터 데이터):
//   날짜 | 노출수 | 클릭수 | 전환수 | 광고비 | 광고기여매출 | ROAS | CTR | 전환율

const CONFIG = {
  SHEET_TAB_NAME: 'naver_shopping_RAW', // ← 실제 네이버 쇼핑 광고 탭명으로 수정
  SHEET_GID:      '',                    // 탭 GID (URL ?gid=XXXXXX에서 확인, 없으면 비워둠)
  HEADER_ROW:     1,                     // 헤더가 있는 행 번호
  DATA_START_ROW: 2,                     // 데이터 시작 행
  BQ_PROJECT:     'aboutfishingproduct',
  BQ_DATASET:     'fishing',
  BQ_TABLE:       'naver_shopping_data',

  // 시트 컬럼명 → BQ 필드 매핑
  // 실제 시트 헤더명이 다르면 오른쪽 값을 맞춰서 수정
  COLUMN_MAP: {
    report_date:  '날짜',           // DATE 형식: YYYY-MM-DD 또는 YYYYMMDD
    impressions:  '노출수',
    clicks:       '클릭수',
    conversions:  '전환수',
    ad_spend:     '광고비',
    revenue:      '광고기여매출',
    roas:         'ROAS',
    ctr:          'CTR',
    cvr_ad:       '전환율'
  }
};

// ── 메인 동기화 함수 ───────────────────────────────────────────────────────────

function syncNaverToBigQuery() {
  try {
    Logger.log('=== 네이버 쇼핑 → BigQuery 동기화 시작 ===');

    const rows = readSheetData();
    if (rows.length === 0) {
      Logger.log('⚠️  시트에 데이터가 없습니다. 동기화 중단.');
      return;
    }

    Logger.log(`📊 시트 데이터: ${rows.length}행`);

    // 기존 데이터 삭제 후 전체 재적재 (TRUNCATE + INSERT)
    truncateBigQueryTable();
    insertRowsToBigQuery(rows);

    Logger.log(`✅ BigQuery 동기화 완료: ${rows.length}행 적재`);
  } catch (e) {
    Logger.log(`❌ 오류: ${e.message}\n${e.stack}`);
    throw e;
  }
}

// ── 시트 데이터 읽기 ───────────────────────────────────────────────────────────

function readSheetData() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  // 탭명으로 시트 찾기, 없으면 GID로 찾기
  let sheet = ss.getSheetByName(CONFIG.SHEET_TAB_NAME);
  if (!sheet) {
    const sheets = ss.getSheets();
    sheet = sheets.find(s => String(s.getSheetId()) === CONFIG.SHEET_GID);
  }
  if (!sheet) {
    throw new Error(`시트 탭을 찾을 수 없습니다: 탭명="${CONFIG.SHEET_TAB_NAME}", GID=${CONFIG.SHEET_GID}`);
  }

  Logger.log(`📋 시트 탭: ${sheet.getName()}`);

  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastRow < CONFIG.DATA_START_ROW) {
    return [];
  }

  // 헤더 읽기
  const headerRange = sheet.getRange(CONFIG.HEADER_ROW, 1, 1, lastCol);
  const headers = headerRange.getValues()[0].map(h => String(h).trim());

  // 컬럼 인덱스 매핑
  const colIdx = {};
  for (const [field, headerName] of Object.entries(CONFIG.COLUMN_MAP)) {
    const idx = headers.indexOf(headerName);
    if (idx === -1) {
      Logger.log(`⚠️  헤더 "${headerName}" 를 찾지 못했습니다. ${field}는 null로 처리됩니다.`);
      colIdx[field] = -1;
    } else {
      colIdx[field] = idx;
    }
  }

  // 데이터 읽기
  const dataRange = sheet.getRange(CONFIG.DATA_START_ROW, 1, lastRow - CONFIG.HEADER_ROW, lastCol);
  const values = dataRange.getValues();

  const rows = [];
  for (const row of values) {
    const dateVal = colIdx.report_date >= 0 ? row[colIdx.report_date] : null;
    if (!dateVal) continue;  // 날짜 없는 행 스킵

    const dateStr = formatDate(dateVal);
    if (!dateStr) continue;

    rows.push({
      report_date:  dateStr,
      impressions:  toInt(colIdx.impressions  >= 0 ? row[colIdx.impressions]  : 0),
      clicks:       toInt(colIdx.clicks       >= 0 ? row[colIdx.clicks]       : 0),
      conversions:  toInt(colIdx.conversions  >= 0 ? row[colIdx.conversions]  : 0),
      ad_spend:     toFloat(colIdx.ad_spend   >= 0 ? row[colIdx.ad_spend]     : 0),
      revenue:      toFloat(colIdx.revenue    >= 0 ? row[colIdx.revenue]      : 0),
      roas:         toFloat(colIdx.roas       >= 0 ? row[colIdx.roas]         : 0),
      ctr:          toFloat(colIdx.ctr        >= 0 ? row[colIdx.ctr]          : 0),
      cvr_ad:       toFloat(colIdx.cvr_ad     >= 0 ? row[colIdx.cvr_ad]       : 0),
    });
  }

  return rows;
}

// ── BigQuery TRUNCATE ────────────────────────────────────────────────────────

function truncateBigQueryTable() {
  const sql = `TRUNCATE TABLE \`${CONFIG.BQ_PROJECT}.${CONFIG.BQ_DATASET}.${CONFIG.BQ_TABLE}\``;
  Logger.log(`🗑️  기존 데이터 삭제: ${sql}`);

  const request = {
    query: sql,
    useLegacySql: false,
    location: 'asia-northeast3'
  };

  const response = BigQuery.Jobs.query(CONFIG.BQ_PROJECT, request);
  if (response.errors && response.errors.length > 0) {
    throw new Error(`TRUNCATE 실패: ${JSON.stringify(response.errors)}`);
  }
  Logger.log('  ✅ TRUNCATE 완료');
}

// ── BigQuery INSERT ────────────────────────────────────────────────────────────

function insertRowsToBigQuery(rows) {
  // BigQuery insertAll은 최대 10,000행 / 10MB 제한 → 500행씩 배치
  const BATCH = 500;
  let inserted = 0;

  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const requestBody = {
      rows: batch.map((r, idx) => ({
        insertId: `${r.report_date}_${i + idx}`,
        json: r
      }))
    };

    const response = BigQuery.Tabledata.insertAll(
      CONFIG.BQ_PROJECT,
      CONFIG.BQ_DATASET,
      CONFIG.BQ_TABLE,
      requestBody
    );

    if (response.insertErrors && response.insertErrors.length > 0) {
      Logger.log(`⚠️  일부 행 삽입 오류: ${JSON.stringify(response.insertErrors.slice(0, 3))}`);
    }

    inserted += batch.length;
    Logger.log(`  → ${inserted}/${rows.length}행 삽입 완료`);
  }
}

// ── 유틸리티 ───────────────────────────────────────────────────────────────────

function formatDate(val) {
  if (!val) return null;
  if (val instanceof Date) {
    const y = val.getFullYear();
    const m = String(val.getMonth() + 1).padStart(2, '0');
    const d = String(val.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
  }
  const s = String(val).trim();
  // YYYY-MM-DD or YYYY/MM/DD
  if (/^\d{4}[-\/]\d{2}[-\/]\d{2}$/.test(s)) {
    return s.replace(/\//g, '-');
  }
  return null;
}

function toInt(v) {
  const n = parseInt(String(v).replace(/,/g, ''), 10);
  return isNaN(n) ? 0 : n;
}

function toFloat(v) {
  const n = parseFloat(String(v).replace(/,/g, ''));
  return isNaN(n) ? 0.0 : n;
}

// ── 수동 테스트용: 시트 데이터 미리보기 ───────────────────────────────────────

function previewSheetData() {
  const rows = readSheetData();
  Logger.log(`총 ${rows.length}행 읽음`);
  rows.slice(0, 5).forEach((r, i) => Logger.log(`[${i+1}] ${JSON.stringify(r)}`));
}
