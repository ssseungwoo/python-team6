import json
import re
from collections import Counter
from classification import predicted_topic, predicted_probability
from crawling import get_crawled_text

# 🔧 설정
idf_path = "/Users/sseung/Documents/study/python_class/project/topic_idf_soynlp_cleaned/정치/idf_values.json"

# ✅ 유효한 명사 필터
def is_valid_token(token):
    return bool(re.fullmatch(r"[가-힣a-zA-Z0-9]{2,}", token))

# ✅ 텍스트에서 명사 후보 추출
def extract_tokens(text):
    tokens = re.findall(r"[가-힣a-zA-Z0-9]{2,}", text)
    return [tok for tok in tokens if is_valid_token(tok)]

# ✅ 입력 문장
text = get_crawled_text()
tokens = extract_tokens(text)
token_counts = Counter(tokens)

# ✅ IDF 값 불러오기
with open(idf_path, "r", encoding="utf-8") as f:
    idf_dict = json.load(f)

# ✅ TF-IDF 계산
tfidf_scores = {}
total_tokens = sum(token_counts.values())

for token, tf in token_counts.items():
    if token in idf_dict:
        idf = idf_dict[token]
        tfidf = (tf / total_tokens) * idf
        tfidf_scores[token] = tfidf

# ✅ 0.5 이상 키워드만 리스트로 저장
keywords_above_threshold = [word for word, score in tfidf_scores.items() if score >= 0.05]

# ✅ 결과 출력
# print("📌 TF-IDF ≥ 0.05 키워드 리스트:")
# print(keywords_above_threshold)

extracted_keywords = keywords_above_threshold

