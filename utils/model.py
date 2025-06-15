import os
import json
import pandas as pd
import numpy as np
import joblib

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder

# ───────────────────────────────────────────────
# 📦 통합 분류기 클래스 정의
class NewsClassifier:
    def __init__(self, model, vectorizer, label_encoder, threshold=0.5):
        self.model = model
        self.vectorizer = vectorizer
        self.label_encoder = label_encoder
        self.threshold = threshold
    
    def predict(self, text):
        X = self.vectorizer.transform([text])
        probs = self.model.predict_proba(X)[0]
        max_prob = np.max(probs)
        pred_index = np.argmax(probs)

        if max_prob < self.threshold:
            return "기타"
        else:
            return self.label_encoder.inverse_transform([pred_index])[0]
    
    def predict_proba(self, text):
        X = self.vectorizer.transform([text])
        probs = self.model.predict_proba(X)[0]
        return dict(zip(self.label_encoder.classes_, probs))

# ───────────────────────────────────────────────
# 📥 뉴스 데이터 불러오기
data_dir = "/Users/sseung/Documents/study/python_class/project_root/data/temp/train/naver"
data = []

for file in os.listdir(data_dir):
    if not file.endswith(".json"):
        continue
    file_path = os.path.join(data_dir, file)
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            articles = json.load(f)  # 리스트 형태라고 가정
            if isinstance(articles, dict):
                articles = [articles]
            for article in articles:
                text = article.get("text") or article.get("content")
                label = article.get("topic")
                if text and label:
                    data.append({"text": text, "label": label})
                else:
                    print(f"❗️필드 누락 in {file}")
    except Exception as e:
        print(f"❗️JSON 로딩 실패: {file} — {e}")

df = pd.DataFrame(data)
print(f"✅ 유효한 뉴스 기사 수: {len(df)}개")

# ───────────────────────────────────────────────
# 🔤 라벨 인코딩
le = LabelEncoder()
y = le.fit_transform(df['label'])

# 🧠 TF-IDF 벡터화
vectorizer = TfidfVectorizer(max_features=5000)
X = vectorizer.fit_transform(df['text'])

# 🤖 로지스틱 회귀 학습
model = LogisticRegression(max_iter=1000, multi_class="multinomial", solver="lbfgs")
model.fit(X, y)

# 🧩 통합 분류기 래핑
classifier = NewsClassifier(model, vectorizer, le, threshold=0.5)

# 💾 모델 저장
save_path = "/Users/sseung/Documents/study/python_class/project_root/model/news_classifier_plusnaver.pkl"
os.makedirs(os.path.dirname(save_path), exist_ok=True)
joblib.dump(classifier, save_path)

print(f"\n✅ 모델 저장 완료: {save_path}")
