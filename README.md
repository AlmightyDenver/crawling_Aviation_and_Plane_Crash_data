# ✈️ Aviation & Plane Crash Data Crawler

> ASN(Aviation Safety Network) 웹사이트([asn.flightsafety.org](https://asn.flightsafety.org/))에서 항공 사고 데이터를 수집하는 Python 기반 크롤러입니다.

<br>
<br>
<br>

## 📂 프로젝트 구조

📦 crawling_Aviation_and_Plane_Crash_data/<br>
├── crawler_asn_data.py # ASN 메인 페이지에서 사고 목록 수집<br>
├── crawler_asn_data_detail.py # 사고 상세 페이지에서 추가 정보 수집

<br>
<br>
<br>

## 🛠️ 사용 방법

1. **필수 라이브러리 설치**:
   `pip install requests beautifulsoup4`
2. **사고 목록 수집 실행**:
`python crawler_asn_data.py`
3. **사고 상세 정보 수집 실행** :
`python crawler_asn_data_detail.py`

💡 수집된 데이터는 CSV 또는 JSON 형식으로 저장되며, 데이터 분석 및 시각화에 활용할 수 있습니다.

<br>
<br>
<br>


## 📌 참고 사항
크롤링 대상 사이트: ASN

크롤링 시 사이트의 robots.txt 및 이용 약관을 준수해주세요.

수집된 데이터는 연구 및 학습 목적으로만 사용하시기 바랍니다.

# 📄 라이선스

이 프로젝트는 [MIT 라이선스](LICENSE)를 따릅니다.
