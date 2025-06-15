import os
import json
import joblib
from konlpy.tag import Mecab
from typing import List

# 📌 키워드 추출 함수 (MeCab 기반 단순 TF 방식)
def extract_keywords(text: str, mecab: Mecab, top_k: int = 5) -> List[str]:
    nouns = mecab.nouns(text)
    freq = {}
    for noun in nouns:
        if len(noun) > 1:
            freq[noun] = freq.get(noun, 0) + 1
    sorted_keywords = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    return [kw for kw, _ in sorted_keywords[:top_k]]

# 📌 모델 클래스 정의
class NewsClassifier:
    def __init__(self, model, vectorizer, label_encoder, threshold=0.5):
        self.model = model
        self.vectorizer = vectorizer
        self.label_encoder = label_encoder
        self.threshold = threshold

    def predict(self, text):
        X = self.vectorizer.transform([text])
        probs = self.model.predict_proba(X)[0]
        max_prob = max(probs)
        pred_index = probs.argmax()
        if max_prob < self.threshold:
            return "기타"
        return self.label_encoder.inverse_transform([pred_index])[0]

    def predict_proba(self, text):
        X = self.vectorizer.transform([text])
        probs = self.model.predict_proba(X)[0]
        return dict(zip(self.label_encoder.classes_, map(float, probs)))

# 📁 경로 설정
model_path = "/Users/sseung/Documents/study/python_class/project_root/model/news_classifier_plusnaver.pkl"
input_base_path = "/Users/sseung/Documents/study/python_class/project_root/data/use/crawling"
output_base_path = "/Users/sseung/Documents/study/python_class/project_root/data/use/crawling"

# 📦 모델 로드
model_obj = joblib.load(model_path)
classifier = model_obj  # 이미 NewsClassifier 인스턴스로 저장되어 있음
mecab = Mecab()

# 📰 언론사 목록
companies = ["kbs", "sbs", "ytn"]

# 🔁 각 언론사별 처리
for company in companies:
    input_path = os.path.join(input_base_path, f"{company}_updated.json")
    output_path = os.path.join(output_base_path, f"{company}_processing_updated.json")

    # 뉴스 데이터 로드
    with open(input_path, "r", encoding="utf-8") as f:
        news_data = json.load(f)

    # 🔍 분류 + 핵심어 추출
    for article in news_data:
        text = article.get("description", "") or article.get("title", "")
        nouns = mecab.nouns(text)
        filtered = " ".join([n for n in nouns if len(n) > 1])

        topic = classifier.predict(filtered)
        prob_dict = classifier.predict_proba(filtered)
        keywords = extract_keywords(text, mecab)

        article["topic"] = topic
        article["probabilities"] = prob_dict
        article["keywords"] = keywords

    # 저장
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(news_data, f, ensure_ascii=False, indent=2)

    print(f"✅ {company.upper()} 처리 완료 → {output_path}")
