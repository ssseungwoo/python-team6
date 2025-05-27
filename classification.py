
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer # https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html
from sklearn.linear_model import LogisticRegression # https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html 설명

model_tfidf_path = "txt/topicwise_top1000_tfidf.csv" #tfidf수치
predict_input_path = "txt/말뭉치_prompts_cleaned.csv" #입력
output_path = "txt/result.csv" #출력

new_data = pd.read_csv(predict_input_path).dropna(subset=["cleaned_prompt"]) #
new_texts = new_data["cleaned_prompt"].astype(str)


data = pd.read_csv(model_tfidf_path).fillna(0) #TF-IDF빈값 0으로
y_labels = data .columns.tolist() 
y_labels.remove("단어") #topic만 가져오기

data_long = data .melt(id_vars="단어", var_name="topic", value_name="score") #단어와 topic 점수를 1대1 매칭 
data_long = data_long[data_long["score"] > 0] # score가 0이면 삭제


tfidf_words = data ["단어"].unique().tolist() #학습할 단어목록

vectorizer = TfidfVectorizer(vocabulary=tfidf_words) #단어를 기반으로 벡터라이저 생성
X_train = vectorizer.fit_transform(data_long["단어"]) # 단어가 X벡터
#희소 행렬로 이루어진 x벡터 희소행렬이 뭔지는 찾아볼것 일단씀
y_train = data_long["topic"] #topic이 y라벨

# 분류 모델 학습
LogisticRegressionmodel = LogisticRegression(max_iter=1000,multi_class='multinomial') #반복횟수는 적당히 크게함 , 다중 클래스 분류 로지스틱함수로 설정
LogisticRegressionmodel.fit(X_train, y_train)# f(x) = y 함수 생성 
 

X_new = vectorizer.transform(new_texts) #예측할 문장을 TF-IDF벡터로 변환 
predictions = LogisticRegressionmodel.predict(X_new) #모델 사용해 topic 예측

# 결과 저장 (컬럼 순서 조정)
new_data["topic"] = predictions
result_data = new_data[["group_id", "topic", "cleaned_prompt"]] #서식 맞추기
result_data.to_csv(output_path, index=False, encoding="utf-8-sig") #출력
print(f"끝: {output_path}")
