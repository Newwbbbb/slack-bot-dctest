
# 던파M 커뮤니티 동향 슬랙 봇 (Patched)

DCInside + Arca Live(아카라이브) 던파M 채널을 통합 수집해 **매일 09:00(KST)** 슬랙으로 요약을 발송합니다.

## 무엇이 바뀌었나요?
- **부분 실패 허용**: Arca 수집 실패 시에도 DCInside 결과는 전송됩니다.
- **헤더 강화**: 브라우저 유사 헤더/리퍼러 적용 → 보호 페이지에 덜 걸림.
- **토글/진단 변수**: `ARCA_ENABLED`, `HOURS`, `REQUEST_INTERVAL_SEC` 등 환경변수로 조정.
- **로그 가독성** 향상.

## 빠른 적용 방법
1. 이 ZIP의 파일로 리포지토리의 **동명 파일을 교체 후 커밋**합니다.
2. 리포지토리 **Settings → Secrets and variables → Actions**
   - Secrets: `SLACK_WEBHOOK_URL` (또는 `SLACK_BOT_TOKEN` + `SLACK_CHANNEL`)
   - Variables(변수): 필요시 아래 추가
     - `ARCA_ENABLED` = `1` (문제시 `0`으로 끄기)
     - `HOURS` = `24` (테스트시 36 등)
     - `REQUEST_INTERVAL_SEC` = `1.5` (문제시 늘려보기)
     - `ARCA_DETAIL_LIMIT` = `20` (본문 조회 상한)
3. **Actions → Daily DNFM Digest → Run workflow**로 수동 테스트.
4. 정상 도착 확인 후, 매일 09:00 KST 자동 발송됩니다.

## 참고
- DCInside 던파M 목록 구조는 표 기반이며(번호/제목/작성일/조회/추천), 본 파서가 이를 사용합니다.
- 아카라이브 던파M 목록 및 개별 글 페이지에는 작성일/조회/추천 메타가 노출되어 파싱에 활용됩니다.

