
# 던파M 갤 동향 슬랙 봇 (GitHub Actions 템플릿)

매일 **오전 9시(KST)**, 던전앤파이터 모바일 마이너 갤러리의 지난 24시간 동향을 요약해 **슬랙 채널**로 보내는 봇입니다.

## 빠른 시작

1. 이 리포지토리(또는 ZIP)를 깃허브에 업로드
2. Slack에서 Incoming Webhook URL 발급 → 리포지토리 **Settings → Secrets → Actions**에 `SLACK_WEBHOOK_URL` 추가
3. (선택) 봇 토큰 방식: `SLACK_BOT_TOKEN`, `SLACK_CHANNEL` 추가
4. 리포지토리 **Actions**에서 워크플로우 수동 실행으로 테스트 → 이후 매일 09:00 KST 자동 실행

## 파일 구성

```
.
├─ app.py                  # 메인 스크립트
├─ requirements.txt        # 의존성
└─ .github/
   └─ workflows/
      └─ daily.yml         # 스케줄 설정 (00:00 UTC = 09:00 KST)
```

## 주의사항
- 사이트 정책(robots.txt, 약관)을 준수하세요. 마크업 변경 시 `parse_list()` 수정 필요.
- 과도한 요청을 피하세요(요청 간격/페이지 제한 코드 반영).
- 깃허브 액션스의 `cron`은 **UTC 기준**입니다.
