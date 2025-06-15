import os
import json
from openai import OpenAI
from dotenv import load_dotenv
from tqdm import tqdm  

# 🔐 API 키 로딩
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")  
client = OpenAI(api_key=api_key)


# 📁 기본 경로 설정
base_dir = "/Users/sseung/Documents/study/python_class/project_root/data/use/process"
companies = ["kbs", "sbs", "ytn"]

# 🧠 요약 함수
def summarize_with_keywords(title, text, keywords):
    prompt = f"""
다음은 뉴스 제목과 본문입니다.

제목: {title}

본문: {text}

위 뉴스에서 다음 핵심어들을 중심으로 3문장 이내로 요약해 주세요:
{', '.join(keywords)}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "너는 유능한 뉴스 요약가야. 사용자 핵심어 중심으로 요약해."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5,
            max_tokens=300
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"⚠️ 요약 실패: {e}")
        return "요약 실패"

# 🔁 언론사별 처리
for company in companies:
    input_path = os.path.join(base_dir, f"{company}_processing_updated.json")
    output_path = os.path.join(base_dir, f"{company.upper()}_processing_summary.json")

    # JSON 열기
    with open(input_path, "r", encoding="utf-8") as f:
        articles = json.load(f)

    # tqdm 진행바 추가
    print(f"📡 {company.upper()} 기사 요약 시작...")
    for article in tqdm(articles, desc=f"{company.upper()} 요약 중", ncols=80):
        title = article.get("title", "")
        text = article.get("description", "")
        keywords = article.get("keywords", [])
        article["summary"] = summarize_with_keywords(title, text, keywords)

    # 저장
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

    print(f"✅ {company.upper()} 요약 완료 → {output_path}\n")
