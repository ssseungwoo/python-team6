import os
import json
import joblib
import numpy as np
from sklearn.manifold import TSNE
from news_classifier import NewsClassifier

# 📁 경로 설정
BASE_DIR = "/Users/sseung/Documents/study/python_class/project_root"
MODEL_PATH = os.path.join(BASE_DIR, "model/news_classifier_allinone.pkl")

# 📰 대상 언론사
companies = ["kbs", "sbs", "ytn"]

# 🎨 토픽별 색상
TOPIC_COLOR = {
    "정치": "#e11d48",
    "경제": "#f59e0b",
    "스포츠": "#10b981",
    "연예": "#a855f7",
    "IT_과학": "#3b82f6",
    "기타": "#6b7280",
}

# ✅ 모델 로드
model: NewsClassifier = joblib.load(MODEL_PATH)

# 🔁 각 언론사별 처리
for company in companies:
    input_path = os.path.join(BASE_DIR, f"data/use/crawling/{company}_crawling_with_summary.json")
    output_path = os.path.join(BASE_DIR, f"data/use/crawling/{company.upper()}_tsne.json")

    # ✅ 데이터 로딩
    with open(input_path, "r", encoding="utf-8") as f:
        articles = json.load(f)

    # ✅ 확률 벡터 기반 X 생성
    X = []
    filtered_articles = []

    for article in articles:
        probs = article.get("probabilities", {})
        if not probs:
            continue
        X.append(list(probs.values()))
        filtered_articles.append(article)

    if not X:
        print(f"⚠️ {company.upper()} 확률 데이터 없음. 건너뜀.")
        continue

    X = np.array(X)

    # ✅ t-SNE 수행
    tsne = TSNE(
        n_components=2,
        perplexity=35,
        early_exaggeration=25,
        learning_rate='auto',
        init='pca',
        random_state=42
    )
    X_embedded = tsne.fit_transform(X)

    # ✅ 결과 변환
    output = []
    for i, article in enumerate(filtered_articles):
        topic = article.get("topic", "기타")
        max_prob = max(article.get("probabilities", {}).values())
        output.append({
            "id": article.get("id", i),
            "title": article.get("title", "")[:50],
            "topic": topic,
            "max_prob": float(max_prob),
            "x": float(X_embedded[i, 0]),
            "y": float(X_embedded[i, 1]),
            "topic_color": TOPIC_COLOR.get(topic, "#6b7280")
        })

    # ✅ 저장
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"✅ {company.upper()} t-SNE 저장 완료 → {output_path}")
