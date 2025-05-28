# import openai
from classification import predicted_topic, predicted_probability
from crawling import get_crawled_text
from tfidf_keyword import extracted_keywords

openai.api_key = os.getenv("OPENAI_API_KEY")

# ✅ 함수: 핵심어 기반 요약 요청
def summarize_news_with_keywords(text, keywords):
#     prompt = f"""
# 다음 뉴스 본문이 있습니다:

# {text}

# 이 뉴스에서 다음의 핵심어들을 중심으로 요약해 주세요:
# {', '.join(keywords)}

# 요약 (3문장 이내):
# """

#     response = client.chat.completions.create(
#         model="gpt-4",
#         messages=[
#             {"role": "system", "content": "너는 유능한 뉴스 요약가야. 사용자 핵심어 중심으로 요약해."},
#             {"role": "user", "content": prompt}
#         ],
#         temperature=0.5,
#         max_tokens=300
#     )

#     return response.choices[0].message.content.strip()
    return "🔧 테스트 중입니다. 이 자리에 요약이 들어갈 예정입니다."


text = get_crawled_text()
keywords = extracted_keywords

summary = summarize_news_with_keywords(text, keywords)

# print("요약 결과: ")
# print(summary)