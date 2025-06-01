import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
import numpy as np

# 파일 경로
model_tfidf_path = "txt/topicwise_top1000_tfidf.csv"
predict_input_path = "update/naver_news_정치_cleaned.csv"
output_path = "update/classified_result.csv"

# 입력 파일 읽기
new_data = pd.read_csv(predict_input_path).dropna(subset=["text"])
new_texts = new_data["text"].astype(str)
true_labels = new_data["topic"]

# TF-IDF 모델 불러오기
data = pd.read_csv(model_tfidf_path).fillna(0)
y_labels = data.columns.tolist()
y_labels.remove("단어")

# 데이터 구조 변경
data_long = data.melt(id_vars="단어", var_name="topic", value_name="score")
data_long = data_long[data_long["score"] > 0]

# 벡터화
tfidf_words = data["단어"].unique().tolist()
vectorizer = TfidfVectorizer(vocabulary=tfidf_words)
X_train = vectorizer.fit_transform(data_long["단어"])
y_train = data_long["topic"]

# 로지스틱 회귀 분류기 학습
model = LogisticRegression(max_iter=1000, multi_class="multinomial")
model.fit(X_train, y_train)

# 예측 수행
X_test = vectorizer.transform(new_texts)
predictions = model.predict(X_test)
proba = model.predict_proba(X_test)

# softmax 확률 열로 추가
proba_df = pd.DataFrame(proba, columns=model.classes_)
result = pd.concat([new_data.reset_index(drop=True), proba_df], axis=1)
result["predicted_topic"] = predictions

# 정확도 출력
print(classification_report(true_labels, predictions))

# 결과 저장
result.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f" 결과 저장 완료: {output_path}")