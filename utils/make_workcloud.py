# import os
# import pandas as pd
# from konlpy.tag import Mecab
# from wordcloud import WordCloud
# import matplotlib.pyplot as plt
# from collections import defaultdict
# import matplotlib

# # ───────────────────────────────────────────────
# # ✅ Mac용 한글 폰트 설정 (한글 깨짐 방지)
# matplotlib.rc("font", family="AppleGothic")
# plt.rcParams["axes.unicode_minus"] = False

# # ───────────────────────────────────────────────
# # 📂 데이터 불러오기 (예측된 결과 CSV)
# data_path = "/Users/sseung/Documents/study/python_class/emer/test_result_from_json.csv"
# df = pd.read_csv(data_path)

# # 📁 워드클라우드 저장 경로
# output_dir = "/Users/sseung/Documents/study/python_class/emer/wordclouds"
# os.makedirs(output_dir, exist_ok=True)

# # 🧠 MeCab 명사 추출기
# mecab = Mecab()

# # ✅ 토픽별 텍스트 분류
# topic_texts = defaultdict(list)
# for _, row in df.iterrows():
#     topic = row.get("predicted_topic") or row.get("label")
#     text = row.get("text") or row.get("prompt")
#     if isinstance(text, str) and isinstance(topic, str):
#         topic_texts[topic].append(text)

# # ✅ 각 토픽별 워드클라우드 생성
# for topic, texts in topic_texts.items():
#     combined = " ".join(texts)
#     nouns = mecab.nouns(combined)
#     filtered = [n for n in nouns if len(n) > 1]
#     final_text = " ".join(filtered)

#     wc = WordCloud(
#         font_path="/Library/Fonts/AppleGothic.ttf",  # Mac 기본 한글 폰트
#         background_color="white",
#         width=800,
#         height=400
#     ).generate(final_text)

#     plt.figure(figsize=(10, 5))
#     plt.imshow(wc, interpolation="bilinear")
#     plt.axis("off")
#     plt.title(f"{topic} 워드클라우드")
#     plt.savefig(os.path.join(output_dir, f"{topic}_wordcloud.png"))
#     plt.close()

# print(f"✅ 워드클라우드 생성 완료: {output_dir}")
    
import os
import pandas as pd
from konlpy.tag import Mecab
from wordcloud import WordCloud
from collections import defaultdict

# 📂 데이터 불러오기
data_path = "/Users/sseung/Documents/study/python_class/emer/test_result_from_json.csv"
df = pd.read_csv(data_path)

# 🧠 MeCab 명사 추출기
mecab = Mecab()

# ✅ 토픽별 텍스트 분류
topic_texts = defaultdict(list)
for _, row in df.iterrows():
    topic = row.get("predicted_topic") or row.get("label")
    text = row.get("text") or row.get("prompt")
    if isinstance(text, str) and isinstance(topic, str):
        topic_texts[topic].append(text)

# ✅ 각 토픽별 단어 가중치만 출력
for topic, texts in topic_texts.items():
    combined = " ".join(texts)
    nouns = mecab.nouns(combined)
    filtered = [n for n in nouns if len(n) > 1]
    final_text = " ".join(filtered)

    wc = WordCloud(
        font_path="/Library/Fonts/AppleGothic.ttf",  # Mac 기본 한글 폰트
        background_color="white",
        width=800,
        height=400
    ).generate(final_text)

    print(f"\n📌 {topic} 토픽의 단어 가중치 (정규화된 값):")
    for word, weight in wc.words_.items():
        print(f"{word}: {weight:.4f}")
