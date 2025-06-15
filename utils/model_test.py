import os
import json
import pandas as pd
import numpy as np
import joblib
from konlpy.tag import Mecab
from sklearn.metrics import classification_report

# ───────────────────────────────────────────────
# 📁 경로 설정
test_file = "/Users/sseung/Documents/study/python_class/project_root/data/news_combined.json"
model_path = "/Users/sseung/Documents/study/python_class/project_root/model/news_classifier_allinone.pkl"
output_path = "/Users/sseung/Documents/study/python_class/project_root/data/temp/test/test_result_from_json.csv"

# ───────────────────────────────────────────────
# 🧠 분류기 클래스
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
        return dict(zip(self.label_encoder.classes_, probs))

# ───────────────────────────────────────────────
# 📦 모델 불러오기
print("📦 모델 로드 중...")
classifier = joblib.load(model_path)

# 🔠 MeCab 로딩
print("🔠 MeCab 로딩 중...")
mecab = Mecab()

def extract_nouns(text):
    if not isinstance(text, str):
        return []
    return [n for n in mecab.nouns(text) if len(n) > 1]

# 📂 테스트셋 로딩
print("📂 테스트셋 로드 중...")
test_df = pd.read_json(test_file)

# ✅ 컬럼 이름 확인 및 사용
text_col = "text" if "text" in test_df.columns else "prompt"
test_df["cleaned_title"] = test_df[text_col]
test_df["cleaned_prompt"] = test_df[text_col]

# 🔧 명사 추출 및 결합
print("🔧 명사 추출 중...")
test_df["processed_text"] = (test_df["cleaned_title"] + " " + test_df["cleaned_prompt"]).apply(
    lambda x: " ".join(extract_nouns(x))
)

# 🤖 예측
print("🤖 예측 중...")
test_df["predicted_topic"] = test_df["processed_text"].apply(classifier.predict)

# 📊 평가
if "topic" in test_df.columns:
    print("\n📊 테스트셋 분류 성능 평가:")
    print(classification_report(test_df["topic"], test_df["predicted_topic"]))
else:
    print("\n🔍 라벨 없음 - 예측 결과:")
    for i, row in test_df.iterrows():
        print(f"[{i+1}] {row['cleaned_title']} → 예측: {row['predicted_topic']}")

# 💾 결과 저장
test_df.to_csv(output_path, index=False)
print(f"\n✅ 결과 저장 완료: {output_path}")
