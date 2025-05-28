import streamlit as st
from PIL import Image
from datetime import datetime, timedelta
from crawling import get_crawled_text
from classification import predicted_topic, predicted_probability
from tfidf_keyword import extracted_keywords
from summarize import summarize_news_with_keywords

# ─────────────────────────────
# ✅ Streamlit 설정

st.set_page_config(page_title="뉴스 요약 및 분류", layout="wide")
st.title("📰 뉴스 요약 및 분야 분류 시스템")
logo_path = "/Users/sseung/Documents/study/python_class/real/file/뉴집스.png"
st.image(Image.open(logo_path), width=120)

# ─────────────────────────────
# 🎛️ 사이드바 구성
st.sidebar.title("📡 뉴스 선택")

# 📺 방송사 선택
broadcaster = st.sidebar.selectbox("방송사", ["KBS", "SBS", "YTN"])

# 🗓️ 날짜 범위 슬라이더 (최근 30일)
today = datetime.today().date()
min_date = today - timedelta(days=30)
default_start = today - timedelta(days=3)
default_end = today

selected_range = st.sidebar.slider(
    "기사 기간 선택",
    min_value=min_date,
    max_value=today,
    value=(default_start, default_end),
    format="YYYY-MM-DD"
)

start_date, end_date = selected_range
st.sidebar.markdown(f"📆 **{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}**")

# 🏷️ 카테고리 선택 버튼 UI
st.sidebar.markdown("### 카테고리 선택")

categories = ["최신순", "경제", "문화", "미용/건강", "사회", "생활", "스포츠", "연예", "정치", "IT/과학"]

# 초기 상태 설정
if "selected_category" not in st.session_state:
    st.session_state.selected_category = "최신순"

# 버튼 스타일 삽입
st.sidebar.markdown("""
<style>
div.stButton > button {
    width: 100%;
    height: 2.4em;
    font-weight: bold;
    border-radius: 6px;
    margin-bottom: 5px;
    border: 1px solid #ccc;
}
div.stButton > button.selected {
    background-color: #4a9cfa;
    color: white;
    border-color: #4a9cfa;
}
</style>
""", unsafe_allow_html=True)

# 버튼 2열 배치
col1, col2 = st.sidebar.columns(2)

for i, cat in enumerate(categories):
    col = col1 if i % 2 == 0 else col2
    is_selected = st.session_state.selected_category == cat

    label = f"✅ {cat}" if is_selected else cat

    # 버튼을 누르면 선택 반영 후 rerun
    if col.button(label, key=f"cat_{cat}"):
        st.session_state.selected_category = cat
        st.rerun()

# 선택된 값 저장
category = st.session_state.selected_category



# ─────────────────────────────
# 📰 뉴스 데이터 (임시 처리)
text = get_crawled_text()
keywords = extracted_keywords
summary = summarize_news_with_keywords(text, keywords)

# ─────────────────────────────
# 🧾 본문 출력
st.markdown(f"**방송사**: {broadcaster}  |  **카테고리**: {category}  |  **기간**: {start_date} ~ {end_date}")

st.subheader("📌 예측된 분야")
st.markdown(f"**{predicted_topic}** ({predicted_probability*100:.2f}%)")

st.subheader("🧠 핵심 키워드 (TF-IDF 기반)")
st.write(", ".join(keywords))

st.subheader("📝 요약 결과")
st.info(summary)

st.subheader("🔍 원문 뉴스 본문")
st.text_area("뉴스 원문", text, height=400)
