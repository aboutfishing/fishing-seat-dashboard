#!/usr/bin/env python3
"""
BigQuery 서비스 계정 이메일 조회 스크립트
Google Sheet 공유 시 필요한 이메일 주소를 출력합니다.

사용법:
  python3 scripts/get_bq_service_account.py

출력 예시:
  BigQuery 서비스 계정 이메일:
  fishing-data@aboutfishingproduct.iam.gserviceaccount.com

  → 이 이메일을 Google Sheet에서 '뷰어' 권한으로 공유하세요.
"""
import json
import os
import sys

def main():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if not creds_path:
        print("❌ GOOGLE_APPLICATION_CREDENTIALS 환경변수가 설정되지 않았습니다.")
        print()
        print("GitHub Actions에서 이 값을 확인하려면:")
        print("  1. GCP Console → IAM → 서비스 계정 탭 이동")
        print("  2. BigQuery 접근 권한이 있는 서비스 계정 이메일 복사")
        print("  3. 해당 이메일을 아래 Google Sheet에서 뷰어로 공유:")
        print("     https://docs.google.com/spreadsheets/d/1AGMVHBMBqNrC-HvLHz7iBAp_wk8kAGSgyPIztD-e-AM")
        sys.exit(0)

    try:
        with open(creds_path) as f:
            key = json.load(f)
        email = key.get("client_email", "")
        project = key.get("project_id", "")

        print(f"✅ BigQuery 서비스 계정 이메일:")
        print(f"   {email}")
        print()
        print(f"   프로젝트: {project}")
        print()
        print("→ 아래 Google Sheet를 이 이메일로 '뷰어' 공유하세요:")
        print("  https://docs.google.com/spreadsheets/d/1AGMVHBMBqNrC-HvLHz7iBAp_wk8kAGSgyPIztD-e-AM/edit")

    except Exception as e:
        print(f"❌ 키 파일 읽기 실패: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
