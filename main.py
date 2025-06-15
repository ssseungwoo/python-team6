from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import json
import os
from collections import Counter, defaultdict
from datetime import datetime

app = FastAPI()

# static 및 templates 디렉토리 설정
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# 데이터 디렉토리
DATA_DIR = "data/use/crawling"
HOT_KEYWORDS_PATH = "data/use/hot_keyword/hot_keywords_by_company.json"

def format_date(date_str):
    try:
        dt = datetime.fromisoformat(date_str)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return date_str

# 1차: 방송사 선택
@app.get("/")
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# 2차: 뉴스 목록 페이지
@app.get("/news/{company}", response_class=HTMLResponse)
async def news_list(request: Request, company: str, sort: str = "latest", filter: str = None):
    filepath = os.path.join(DATA_DIR, f"{company}_crawling_with_summary.json")
    with open(filepath, "r", encoding="utf-8") as f:
        all_articles = json.load(f)

    topics = sorted(set(a.get("topic", "기타") for a in all_articles), key=lambda x: (x == "기타", x))
    articles = all_articles.copy()

    # 날짜 포맷 적용
    for article in articles:
        raw_date = article.get("upload_date_kst", "")
        article["formatted_date"] = format_date(raw_date)

    # 정렬
    if sort == "latest":
        articles.sort(key=lambda x: x.get("upload_date_kst") or "", reverse=True)
    elif sort == "topic":
        articles.sort(key=lambda x: ((x.get("topic") or "") == "기타", x.get("topic") or ""))
    elif sort == "views":
        articles.sort(key=lambda x: x.get("view_count") or 0, reverse=True)

    # 필터
    if filter:
        articles = [a for a in articles if a.get("topic") == filter]
        wordcloud_url = f"/static/wordclouds/{filter}.png"
    else:
        wordcloud_url = None

    # 🔥 실시간 핫 키워드 로딩
    try:
        with open(HOT_KEYWORDS_PATH, "r", encoding="utf-8") as f:
            all_hot_keywords = json.load(f)
        hot_keywords = all_hot_keywords.get(company, {})
    except Exception:
        hot_keywords = {}

    return templates.TemplateResponse("article_list.html", {
        "request": request,
        "company": company,
        "articles": articles,
        "topics": topics,
        "current_topic": filter,
        "sort": sort,
        "wordcloud_url": wordcloud_url,
        "hot_keywords": hot_keywords
    })

# 3차: 뉴스 상세
@app.get("/news/{company}/article/{article_id}", response_class=HTMLResponse)
async def article_detail(request: Request, company: str, article_id: int):
    data_path = os.path.join(DATA_DIR, f"{company}_crawling_with_summary.json")
    tsne_path = os.path.join(DATA_DIR, f"{company}_tsne.json")

    with open(data_path, "r", encoding="utf-8") as f:
        articles = json.load(f)

    with open(tsne_path, "r", encoding="utf-8") as f:
        tsne_data = json.load(f)

    article = next((a for a in articles if a["id"] == article_id), None)
    tsne_point = next((p for p in tsne_data if p["id"] == article_id), None)

    if not article or not tsne_point:
        return HTMLResponse("❌ 기사를 찾을 수 없습니다", status_code=404)

    # ✅ 유사도 계산 (간단히 유클리디안 거리 기준)
    from math import sqrt
    distances = []
    for p in tsne_data:
        if p["id"] == article_id:
            continue
        dist = sqrt((p["x"] - tsne_point["x"])**2 + (p["y"] - tsne_point["y"])**2)
        distances.append((dist, p["id"]))

    # 거리순으로 정렬
    distances.sort(key=lambda x: x[0])
    similar_articles = []

    for dist, sim_id in distances[:5]:
        matched = next((a for a in articles if a["id"] == sim_id), None)
        if matched:
            enriched = matched.copy()
            enriched["distance"] = round(dist * 10, 1)
            similar_articles.append(enriched)

    return templates.TemplateResponse("article_detail.html", {
        "request": request,
        "company": company,
        "article": article,
        "similar_articles": similar_articles
    })

# 4차: t-SNE 시각화
@app.get("/news/{company}/visualization", response_class=HTMLResponse)
def tsne_visualization(request: Request, company: str, highlight: int = None):
    tsne_path = os.path.join(DATA_DIR, f"{company}_tsne.json")
    if not os.path.exists(tsne_path):
        return HTMLResponse("❌ 시각화 데이터 없음", status_code=404)

    with open(tsne_path, "r", encoding="utf-8") as f:
        tsne_data = json.load(f)

    topic_counts = Counter(item.get("topic") or "기타" for item in tsne_data)

    TOPIC_COLOR = {
        "정치": "#e11d48",
        "경제": "#f59e0b",
        "스포츠": "#10b981",
        "연예": "#a855f7",
        "IT_과학": "#3b82f6",
        "기타": "#6b7280"
    }

    for item in tsne_data:
        item["topic_color"] = TOPIC_COLOR.get(item.get("topic"), "#6b7280")

    return templates.TemplateResponse("tsne_visualization.html", {
        "request": request,
        "company": company,
        "tsne_data_json": tsne_data,
        "topic_counts": topic_counts,
        "topic_colors": TOPIC_COLOR,
        "highlight_id": highlight if highlight is not None else None
    })
