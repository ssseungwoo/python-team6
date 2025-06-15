import os
import json
from collections import Counter, defaultdict
from konlpy.tag import Mecab

# 기본 설정
DATA_DIR = "/Users/sseung/Documents/study/python_class/project_root/data/use/crawling"
OUTPUT_PATH = "/Users/sseung/Documents/study/python_class/project_root/data/use/hot_keyword/hot_keywords_by_company.json"
BROADCASTERS = ["KBS", "SBS", "YTN"]
TOPICS = ["IT_과학", "경제", "정치", "스포츠", "연예", "기타"]

# 불용어 정의 (원하는 대로 더 추가 가능)
stopwords = set([
    "것", "있다", "없다", "입니다", "대한", "한다", "하며", "수", "합니다", "겁니다", "되다",
    "기자", "영상", "뉴스", "보도", "앵커", "말하다", "통해", "위해", "에서", "으로", "이다", "하는",
    "같다", "그리고", "하지만", "그러나", "겁니다"
])

mecab = Mecab()
result = {b: {t: [] for t in TOPICS} for b in BROADCASTERS}

for broadcaster in BROADCASTERS:
    file_path = os.path.join(DATA_DIR, f"{broadcaster}_crawling_with_summary.json")
    if not os.path.exists(file_path):
        print(f"❌ {file_path} 없음. 건너뜁니다.")
        continue

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            articles = json.load(f)
    except Exception as e:
        print(f"⚠️ {broadcaster} 파일 로딩 중 오류: {e}")
        continue

    topic_word_counts = defaultdict(Counter)

    for article in articles:
        try:
            topic = article.get("topic", "기타")
            if topic not in TOPICS:
                topic = "기타"

            text = article.get("cleaned_description", "") + " " + article.get("cleaned_title", "")
            if not text.strip():
                continue

            nouns = [
                word for word, tag in mecab.pos(text)
                if tag.startswith("NN") and word not in stopwords and len(word) > 1
            ]
            topic_word_counts[topic].update(nouns)

        except Exception as e:
            print(f"🛑 기사 분석 중 오류 발생 (ID: {article.get('id', 'N/A')}): {e}")
            continue

    for topic in TOPICS:
        result[broadcaster][topic] = [
            word for word, _ in topic_word_counts[topic].most_common(10)
        ]

# 저장
try:
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"✅ 핫 키워드 저장 완료: {OUTPUT_PATH}")
except Exception as e:
    print(f"❌ 저장 실패: {e}")
