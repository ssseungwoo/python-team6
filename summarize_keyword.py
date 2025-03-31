import requests
from bs4 import BeautifulSoup
from collections import Counter
import re

#뉴스 섹션 URL
url = "https://news.naver.com/section/100"  #sid만 바꿔주면 카테고리 변경가능
data = requests.get(url)
soup = BeautifulSoup(data.text, "html.parser") #soup객체 생성

#기사 제목 추출
titles = soup.select("strong.sa_text_strong")
all_titles = " ".join([title.get_text(strip=True) for title in titles])

#단어 추출 (2글자 이상 한글)
words = re.findall(r'\b[가-힣]{2,}\b', all_titles)

#단어 빈도 분석
counts = Counter(words)
result_ = counts.most_common(10)

#결과 출력
print("\n오늘의 주요 키워드 (상위 10개):")
for word, count in result_:
    print(f"{word}: {count}번")
