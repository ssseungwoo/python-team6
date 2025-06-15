import json

# 기존 및 신규 파일 경로
original_path = "project_root/data/use/crawling/SBS_crawling_with_summary.json"      # 기존 뉴스 파일
new_path = "project_root/data/use/process/SBS_processing_summary.json"       # 새로 추가할 뉴스 파일
output_path = "project_root/data/use/crawling/SBS_crawling_with_summary.json"     # 저장할 병합된 파일

# 파일 불러오기
with open(original_path, "r", encoding="utf-8") as f:
    original_data = json.load(f)

with open(new_path, "r", encoding="utf-8") as f:
    new_data = json.load(f)

# 기존 데이터의 마지막 id 값 확인
existing_ids = [item.get("id", 0) for item in original_data if isinstance(item.get("id"), int)]
max_id = max(existing_ids) if existing_ids else 0

# 새 데이터에 id 재할당
for i, article in enumerate(new_data, start=1):
    article["id"] = max_id + i

# 병합
merged_data = original_data + new_data

# 저장
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(merged_data, f, ensure_ascii=False, indent=2)

print(f"✅ 병합 완료: 총 {len(merged_data)}개 → {output_path}")
