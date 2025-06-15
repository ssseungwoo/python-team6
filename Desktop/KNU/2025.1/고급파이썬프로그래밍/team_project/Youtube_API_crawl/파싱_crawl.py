import asyncio
import json
import re
from datetime import datetime, timezone, timedelta

import aiohttp
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from dateutil import parser
from konlpy.tag import Okt # 형태소 분석기

# --- 사용자 제공 전처리 함수들 ---
okt = Okt() # Okt 형태소 분석기 인스턴스 (전역 변수로 한 번만 생성)

def clean_title(title):
    if not isinstance(title, str):
        return ""
    title = re.sub(r"\s*/\s*(KBS|SBS|YTN).*", "", title)
    title = re.sub(r"\([^)]*\)", "", title)
    title = re.sub(r"\[[^\]]*\]", "", title)
    title = re.sub(r"\{[^}]*\}", "", title)
    title = re.sub(r"【[^】]*】", "", title)
    title = re.sub(r'([가-힣ㅏ-ㅣ])\1{1,}', '', title)
    title = re.sub(r'[ㅋㅎㅠㅜ]{2,}', '', title)
    title = re.sub(r'앗|헉|윽|흥|풉|에구|읏|으음|아악|끼야|푸하하|하하하|히히히|헤헤헤|흐흐흐|낄낄|깔깔|콜록콜록|훌쩍|쉿', '', title)
    title = re.sub(r"[^\w\s가-힣.%]", " ", title)
    title = re.sub(r"[·•]{3,}|\.{3,}|…", " ", title)
    title = re.sub(r"\s+", " ", title)
    return title.strip()

def clean_text_full(text):
    if not isinstance(text, str):
        return ""
    text = re.sub(r'^(▣|◇|■|▲|▼|◆|○|●|△|▷|▶|※|◎)\s*', ' ', text)
    text = re.sub(r'\([^()]*\)', ' ', text)
    text = re.sub(r'\{[^{}]*\}', ' ', text)
    text = re.sub(r'\[[^\[\]]*\]', ' ', text)
    lines = text.splitlines()
    lines = [line for line in lines if not line.strip().startswith(('▣', '◇', '■', '▲', '▼', '◆', '○', '●', '△', '▷', '▶', '※', '◎'))]
    text = ' '.join(lines)
    text = re.sub(r'([가-힣]{2,4})(?:[이|임|섭|립|음|합|습|랍]?)\b니다', ' ', text)
    text = re.sub(r'([가-힣]{2,4})(?:[이|임|섭|립|음|합|습|랍]?)\b습니다', ' ', text)
    job_titles = '기자|앵커|특파원|기상캐스터|진행|촬영기자|그래픽|리포터|논설위원|논평|해설위원|취재|현장기자|뉴스팀|보도국|영상편집'
    text = re.sub(rf'([가-힣]{{2,4}}\s*(?:{job_titles})(?:\s*[:/]\s*[가-힣]{{2,4}}\s*(?:{job_titles})?)*)', ' ', text)
    text = re.sub(rf'((?:{job_titles})\s*[:/]\s*[가-힣]{{2,4}}(?:\s*[:/]\s*(?:{job_titles})?\s*[가-힣]{{2,4}})*)', ' ', text)
    endings = '입니다|입니다\\.|입니다\\?|이다|입니다만|입니다만\\.|이라고|이라는|라고|는|은|가|이었습니다\\.|이었습니다|였습니다\\.|였습니다'
    text = re.sub(rf'((?:{job_titles})\s*[:/]\s*[가-힣]{{2,4}}(?:\s*[:/]\s*(?:{job_titles})?\s*[가-힣]{{2,4}})*)', ' ', text)
    text = re.sub(r'\'[가-힣\s]+\'\s*[가-힣]{2,10}(?:[ ]*였습니다\\.|였습니다|이었습니다\\.|이었습니다)\s*', ' ', text)
    text = re.sub(r'[가-힣]{2,4}의\s+[가-힣\s]+(?:[ ]*였습니다\\.|였습니다|이었습니다\\.|이었습니다)\s*', ' ', text)
    text = re.sub(rf'([가-힣]{{2,4}}\s*(?:{job_titles})\s*(?:{endings})\s*)', ' ', text)
    text = re.sub(rf'([가-힣]{{2,4}}\s*(?:{job_titles})\s*)', ' ', text)
    text = re.sub(r'\b(기자|앵커|특파원|기상캐스터|진행|촬영기자|그래픽|리포터|논설위원|논평|해설위원|취재|현장기자|뉴스팀|보도국)\b', ' ', text)
    text = re.sub(r'(뉴스\s?[가-힣]{1,10}입니다[.]?)|(기자입니다[.]?)|(기잡니다[.]?)|(보도합니다[.]?)|(전합니다[.]?)|(전해드립니다[.]?)', ' ', text)
    text = re.sub(r'[가-힣]{2,10}\s*:\s*[가-힣\s]{2,100}', ' ', text)
    text = re.sub(r'[가-힣]{2,7}\s*:\s*[가-힣]{2,6}(?:[ /·ㆍ,][가-힣]{2,6})*', ' ', text)
    text = re.sub(r'["“”‘’\'`]', ' ', text)
    text = re.sub(r'(KBS|SBS|YTN)\s뉴스\s[가-힣]{1,4}((입|이|합|습|랍)?입니다)', ' ', text)
    text = re.sub(r'\b(KBS|SBS|YTN)\b', ' ', text)
    text = re.sub(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?[가-힣a-zA-Z%℃]+\b', ' ', text)
    text = re.sub(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text.strip()

def clean_text_for_vectorization(text):
    if not isinstance(text, str):
        return ""
    cleaned_initial_text = clean_text_full(text)
    cleaned_initial_text = re.sub(r'([가-힣ㅏ-ㅣ])\1{1,}', '', cleaned_initial_text)
    cleaned_initial_text = re.sub(r'[ㅋㅎㅠㅜ]{2,}', '', cleaned_initial_text)
    cleaned_initial_text = re.sub(r'[^\w\s가-힣.]', ' ', cleaned_initial_text)
    cleaned_initial_text = re.sub(r'\s+', ' ', cleaned_initial_text).strip()
    words = []
    for word, tag in okt.pos(cleaned_initial_text, norm=True, stem=True):
        if tag in ['Noun', 'Verb', 'Adjective', 'Adverb']:
            words.append(word)
    korean_stopwords = [
        '오늘', '이번', '지난', '되다', '하다', '있다', '이다', '것', '수', '그', '더', '좀', '잘',
        '가장', '다', '또', '많이', '그리고', '그러나', '하지만', '따라', '등', '등등', '통해',
        '까지', '부터', '대한', '으로', '에서', '에게', '에게서', '보다', '때문',
        '습니다', '합니다', '합니다만', '입니다만', '이라고', '이었습니다', '였습니다',
        '년', '월', '일', '시', '분', '초', '오전', '오후', '이번주', '지난주', '다음주', '이달', '지난달', '다음달', '올해', '지난해', '내년'
    ]
    words = [word for word in words if word not in korean_stopwords and len(word) > 1]
    return ' '.join(words)

# --- 유틸리티 함수 (기존과 동일) ---

def parse_duration_to_seconds(duration_str):
    """
    ISO 8601 기간 문자열 (예: PT96S, PT1M30S)을 초 단위로 변환합니다.
    Selenium으로 받은 "X분 Y초" 형태도 처리합니다.
    """
    if isinstance(duration_str, int): # 이미 초 단위 정수일 경우
        return duration_str
    
    duration_str = str(duration_str).upper()

    # ISO 8601 Duration (e.g., PT96S, PT1M30S)
    iso_match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
    if iso_match:
        hours, minutes, seconds = iso_match.groups()
        total_seconds = 0
        if hours:
            total_seconds += int(hours) * 3600
        if minutes:
            total_seconds += int(minutes) * 60
        if seconds:
            total_seconds += int(seconds)
        return total_seconds
    
    # "X분 Y초" (e.g., "1분 30초", "20초")
    korean_match = re.match(r'(?:(\d+)분\s*)?(?:(\d+)초)?', duration_str)
    if korean_match:
        minutes_korean, seconds_korean = korean_match.groups()
        total_seconds = 0
        if minutes_korean:
            total_seconds += int(minutes_korean) * 60
        if seconds_korean:
            total_seconds += int(seconds_korean)
        return total_seconds

    return 0 # 파싱 실패 시 0 반환

def convert_upload_date_to_kst(iso_datetime_str):
    """
    ISO 8601 형식의 날짜 문자열을 KST (UTC+9)로 변환합니다.
    """
    try:
        dt_with_tz = parser.isoparse(iso_datetime_str)
        kst_timezone = timezone(timedelta(hours=9))
        dt_kst = dt_with_tz.astimezone(kst_timezone)
        return dt_kst.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"⛔ uploadDate 시간 파싱 실패: {e}")
        return ""

# --- YouTube 크롤러 클래스 ---

class YouTubeNewsCrawler:
    def __init__(self, channel_url, max_videos_to_collect=200, min_duration_sec=60, max_duration_sec=300, recent_hours=24):
        self.channel_url = channel_url
        self.max_videos_to_collect = max_videos_to_collect
        self.min_duration_sec = min_duration_sec
        self.max_duration_sec = max_duration_sec
        self.recent_hours = recent_hours
        self.driver = None # Selenium WebDriver 인스턴스
        self.collected_links = set() # 중복 링크 방지
        self.results = [] # 최종 결과 저장 (1차 필터링 후)
        
        # 크롤링 시작 시간을 KST로 기록 (24시간 필터링 기준)
        self.crawl_start_time_kst = datetime.now(timezone(timedelta(hours=9)))

    def _init_driver(self):
        """Selenium WebDriver를 초기화합니다."""
        print("🌐 WebDriver 초기화 중...")
        options = Options()
        options.add_argument("--headless")  # GUI 없이 백그라운드 실행
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        options.add_argument("lang=ko_KR") # 한국어 설정
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        print("✅ WebDriver 초기화 완료.")

    def _close_driver(self):
        """Selenium WebDriver를 종료합니다."""
        if self.driver:
            print("👋 WebDriver 종료 중...")
            self.driver.quit()
            self.driver = None
            print("✅ WebDriver 종료 완료.")

    async def _scroll_to_end(self):
        """페이지를 끝까지 스크롤하여 동적 콘텐츠를 로드합니다."""
        last_height = self.driver.execute_script("return document.documentElement.scrollHeight")
        scroll_attempts = 0
        max_scroll_attempts = 100 # 무한 스크롤 방지를 위한 최대 스크롤 시도 횟수
        
        while True:
            self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            await asyncio.sleep(2) # 페이지 로드를 위해 잠시 대기
            
            new_height = self.driver.execute_script("return document.documentElement.scrollHeight")
            if new_height == last_height:
                scroll_attempts += 1
                if scroll_attempts > 3: # 3번 스크롤해도 새 콘텐츠 없으면 종료
                    break
            else:
                scroll_attempts = 0 # 새 콘텐츠 로드 시 시도 횟수 초기화
            last_height = new_height
            
            # 수집 목표 영상 개수 이상이 로드되면 스크롤 중단
            if len(self.driver.find_elements(By.CSS_SELECTOR, "ytd-rich-item-renderer")) >= self.max_videos_to_collect:
                print(f"🌟 {self.max_videos_to_collect}개 이상의 영상이 로드되어 스크롤 중단.")
                break
            
            if scroll_attempts >= max_scroll_attempts:
                print(f"⚠️ 최대 스크롤 시도 횟수({max_scroll_attempts}) 도달, 스크롤 중단.")
                break


    async def collect_video_links_and_filter_length(self):
        """
        Selenium으로 채널 페이지를 스크롤하며 영상 링크와 제목을 수집하고,
        영상 길이(1~5분)를 1차 필터링합니다.
        """
        print(f"🎬 채널 동영상 탭 접속: {self.channel_url}")
        
        await asyncio.to_thread(self._init_driver) # WebDriver 초기화는 동기 함수이므로 to_thread 사용
        
        try:
            await asyncio.to_thread(self.driver.get, self.channel_url)
            
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.ID, "contents"))
            )
            print("✅ 채널 페이지 로드 완료. 영상 목록 수집 시작...")

            await self._scroll_to_end() # 비동기 스크롤

            video_elements = await asyncio.to_thread(self.driver.find_elements, By.CSS_SELECTOR, "ytd-rich-item-renderer")
            print(f"🔍 총 {len(video_elements)}개의 영상 요소를 찾았습니다.")

            for i, element in enumerate(video_elements):
                if len(self.results) >= self.max_videos_to_collect:
                    print(f"🔥 목표 영상 개수({self.max_videos_to_collect}) 도달, 수집 중단.")
                    break

                try:
                    link_element = element.find_element(By.CSS_SELECTOR, "a#video-title-link")
                    video_link = link_element.get_attribute("href")
                    if not video_link: continue

                    if video_link.startswith("/watch"):
                        video_link = f"https://www.youtube.com{video_link}"
                    
                    if video_link in self.collected_links: continue

                    title = link_element.get_attribute("title") or link_element.text
                    if not title: continue

                    # 영상 길이 추출 및 1차 필터링
                    duration_text = None
                    try:
                        # 썸네일 위에 표시되는 시간 정보 추출
                        duration_element = element.find_element(By.CSS_SELECTOR, "ytd-thumbnail-overlay-time-status-renderer #text")
                        duration_text = duration_element.text.strip()
                    except:
                        pass 

                    video_duration_sec = parse_duration_to_seconds(duration_text)

                    if not (self.min_duration_sec <= video_duration_sec < self.max_duration_sec):
                        # print(f"⏳ 영상 길이 필터링: '{title}' ({duration_text}, {video_duration_sec}초). 조건 불일치.")
                        continue 

                    self.collected_links.add(video_link)
                    
                    # 1차 필터링 통과한 정보 저장 (나중에 상세 정보 채움)
                    self.results.append({
                        'title': title,
                        'link': video_link
                    })
                    print(f"✅ 1차 필터링 통과: '{title}' ({duration_text})")

                except Exception as e:
                    print(f"⚠️ 영상 요소 처리 중 오류 발생: {e} - 요소 {i+1}")
                    continue
            
            print(f"✨ 1차 필터링 후 수집된 영상 링크 개수: {len(self.results)}개")

        finally:
            await asyncio.to_thread(self._close_driver) # 드라이버 종료

    async def get_video_details_from_link(self, video_info):
        """
        개별 영상 페이지에 접속하여 JSON-LD에서 상세 정보(프롬프트, 업로드 시간 등)를 추출하고,
        업로드 시간(24시간 이내)을 2차 필터링합니다.
        """
        video_link = video_info['link']
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(video_link) as response:
                    response.raise_for_status() # HTTP 오류 발생 시 예외 발생
                    html_content = await response.text()

            soup = BeautifulSoup(html_content, 'html.parser')
            
            # JSON-LD 데이터 추출
            json_ld_script = soup.find('script', type='application/ld+json')
            
            if not json_ld_script:
                # print(f"❌ JSON-LD 스크립트 없음: {video_link}")
                return None 

            json_data = json.loads(json_ld_script.string)

            # --- JSON-LD에서 정보 추출 (사용자 요청 필드만) ---
            
            # 1. 업로드 시간 (uploadDate) - 2차 필터링 기준
            upload_date_iso = json_data.get('uploadDate')
            if not upload_date_iso:
                # print(f"❌ uploadDate 없음: {video_link}")
                return None

            upload_dt_kst = parser.isoparse(upload_date_iso).astimezone(timezone(timedelta(hours=9)))
            
            # 2차 필터링: 24시간 이내 조건
            time_difference = self.crawl_start_time_kst - upload_dt_kst
            if time_difference.total_seconds() > (self.recent_hours * 3600):
                # print(f"⏰ 업로드 시간 필터링: '{video_info['title']}' ({upload_dt_kst.strftime('%Y-%m-%d %H:%M:%S KST')}). 24시간 초과.")
                return None 

            # 2. 영상 설명 (프롬프트) - JSON-LD에서 직접 가져옴
            raw_prompt = json_data.get('description', '')

            # 3. 썸네일 URL
            thumbnail_urls = json_data.get('thumbnailUrl', [])
            thumbnail_link = thumbnail_urls[0] if thumbnail_urls else ''
            
            # --- 데이터 전처리 ---
            cleaned_title = clean_title(video_info['title'])
            cleaned_prompt = clean_text_for_vectorization(raw_prompt)
            
            # 최종 결과 반환 (요청하신 필드만 포함)
            return {
                'id': None, # ID는 나중에 DataFrame에서 부여
                'raw_title': video_info['title'],
                'cleaned_title': cleaned_title,
                'raw_prompt': raw_prompt,
                'cleaned_prompt': cleaned_prompt,
                'link': video_link,
                'upload_date_kst': upload_dt_kst.strftime("%Y-%m-%d %H:%M:%S"),
                'thumbnail_link': thumbnail_link
            }

        except aiohttp.ClientError as e:
            print(f"❌ HTTP 요청 오류 ({video_link}): {e}")
        except json.JSONDecodeError as e:
            print(f"❌ JSON 파싱 오류 ({video_link}): {e}")
        except Exception as e:
            print(f"⚠️ 개별 영상 상세 정보 추출 중 오류 발생 ({video_link}): {e}")
        return None

    async def run_crawler(self):
        """크롤러의 전체 실행 흐름을 관리합니다."""
        print(f"🚀 크롤링 시작: {self.crawl_start_time_kst.strftime('%Y-%m-%d %H:%M:%S KST')} 기준 {self.recent_hours}시간 이내 영상 수집")
        
        start_time = datetime.now()

        # 1단계: Selenium으로 링크 수집 및 1차 길이 필터링
        await self.collect_video_links_and_filter_length()

        if not self.results:
            print("🚫 1차 필터링 후 수집된 영상이 없습니다. 크롤링 종료.")
            return pd.DataFrame()

        print(f"\n⚡ 2단계: 수집된 {len(self.results)}개 영상의 상세 정보 추출 및 2차 시간 필터링 시작...")
        
        final_video_data = []
        for i, video_info in enumerate(self.results):
            print(f"처리 중... ({i+1}/{len(self.results)}) - {video_info['title']}")
            details = await self.get_video_details_from_link(video_info)
            if details:
                final_video_data.append(details)

        if not final_video_data:
            print("🚫 모든 필터링을 통과한 영상이 없습니다.")
            return pd.DataFrame()

        # Pandas DataFrame으로 변환
        df = pd.DataFrame(final_video_data)
        df['id'] = range(1, len(df) + 1) # 순차적 ID 부여
        
        end_time = datetime.now()
        total_time = end_time - start_time
        
        print(f"\n🎉 크롤링 완료! 총 {len(df)}개의 영상 정보를 수집했습니다.")
        print(f"총 소요 시간: {total_time}")
        
        # 결과 저장 (선택 사항)
        output_filename = "youtube_news_videos_crawled.json"
        df.to_json(output_filename, orient="records", indent=4, force_ascii=False)
        print(f"결과가 '{output_filename}' 파일로 저장되었습니다.")
        
        return df

# --- 메인 실행 ---

async def main():
    # 실제 크롤링할 YouTube 채널의 동영상 탭 URL을 여기에 입력하세요.
    # 이전에 'https://www.youtube.com/@newskbs/videos' 로 주셨으나, 이는 예시이며 실제 작동하는 URL로 변경해야 합니다.
    target_channel_url = "https://www.youtube.com/@newskbs/videos" 
    
    # 크롤링 설정 (파라미터 조정 가능)
    crawler = YouTubeNewsCrawler(
        channel_url=target_channel_url,
        max_videos_to_collect=50,  # 1차 필터링 전 최대 수집 시도할 영상 개수 (테스트 시 적게 설정)
        min_duration_sec=60,      # 최소 영상 길이 (1분)
        max_duration_sec=300,     # 최대 영상 길이 (5분)
        recent_hours=24           # 최근 업로드 시간 (24시간 이내)
    )
    
    crawled_df = await crawler.run_crawler()
    
    if not crawled_df.empty:
        print("\n--- 수집된 데이터 미리보기 ---")
        print(crawled_df[['id', 'cleaned_title', 'upload_date_kst', 'duration_sec', 'link']].head())
        print(f"\n최종 DataFrame 크기: {len(crawled_df)}개 영상")
    else:
        print("수집된 데이터가 없습니다.")

if __name__ == "__main__":
    asyncio.run(main())