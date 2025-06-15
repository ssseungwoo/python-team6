import time
import re
import pandas as pd
import json
import asyncio # 비동기 처리를 위한 모듈
import aiohttp # 비동기 HTTP 요청을 위한 라이브러리 (선택 사항, 상세 페이지 크롤링에 활용)
from bs4 import BeautifulSoup # HTML 파싱을 위한 라이브러리 (선택 사항, 상세 페이지 크롤링에 활용)

from datetime import datetime, timezone, timedelta
from dateutil import parser

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from webdriver_manager.chrome import ChromeDriverManager

from konlpy.tag import Okt

# Okt 객체는 한 번만 생성하여 재사용하는 것이 효율적입니다.
okt = Okt()

# --- 기존 전처리 함수들 (동일하게 사용) ---
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

def clean_text_full(text, apply_morphs=False):
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

def convert_upload_date_to_kst(iso_datetime_str):
    try:
        dt_with_tz = parser.isoparse(iso_datetime_str)
        dt_kst = dt_with_tz.astimezone(timezone(timedelta(hours=9)))
        return dt_kst.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print("⛔ uploadDate 시간 파싱 실패:", e)
        return ""

### **비동기 크롤러 클래스 정의**

class YouTubeNewsCrawler:
    def __init__(self, channel_url, max_videos_to_collect=10, max_scrolls=10):
        self.channel_url = channel_url
        self.max_videos_to_collect = max_videos_to_collect
        self.max_scrolls = max_scrolls
        self.link_queue = asyncio.Queue()  # 링크 정보를 담을 큐
        self.result_queue = asyncio.Queue() # 최종 결과 데이터를 담을 큐
        self.seen_titles = set()
        self.driver = None # Selenium WebDriver 인스턴스

    async def _init_driver(self):
        """WebDriver를 초기화하는 비동기 함수"""
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--lang=ko-KR")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        
        # Selenium WebDriver 초기화는 동기 함수이므로 to_thread를 사용
        self.driver = await asyncio.to_thread(
            webdriver.Chrome, service=Service(ChromeDriverManager().install()), options=options
        )
        print("✅ WebDriver 초기화 완료.")

    async def _close_driver(self):
        """WebDriver를 종료하는 비동기 함수"""
        if self.driver:
            await asyncio.to_thread(self.driver.quit)
            print("✅ WebDriver 종료.")
            self.driver = None

    async def link_collector(self):
        """
        채널 페이지에서 영상 링크, 제목, 썸네일을 비동기적으로 수집하여 큐에 넣는 코루틴.
        Selenium 작업은 to_thread를 통해 별도 스레드에서 실행.
        """
        print(f"✅ YouTube 채널에 접속 중: {self.channel_url}")
        await asyncio.to_thread(self.driver.get, self.channel_url)
        
        try:
            await asyncio.to_thread(
                WebDriverWait(self.driver, 10).until,
                EC.presence_of_element_located((By.ID, "contents"))
            )
            print("✅ 채널 로딩 완료.")
        except TimeoutException:
            print("⛔ 채널 로딩 시간 초과.")
            return

        scroll_pause = 1.0
        scroll_count = 0
        last_height = await asyncio.to_thread(self.driver.execute_script, "return document.documentElement.scrollHeight")

        print("✅ 조건에 맞는 영상 정보 수집 중...")
        while len(self.seen_titles) < self.max_videos_to_collect and scroll_count < self.max_scrolls:
            # 동기적인 Selenium find_elements 호출을 to_thread로 감쌈
            video_elements = await asyncio.to_thread(
                self.driver.find_elements, By.CSS_SELECTOR, "ytd-rich-item-renderer"
            )

            for element in video_elements:
                if len(self.seen_titles) >= self.max_videos_to_collect:
                    break
                
                try:
                    # Selenium 요소 속성 접근 및 추출도 to_thread로 감싸는 것이 안전
                    badge_aria = await asyncio.to_thread(
                        lambda: element.find_element(By.CSS_SELECTOR, "badge-shape[aria-label]").get_attribute("aria-label")
                    )
                    match_min = re.search(r"(\d+)분", badge_aria)
                    match_sec = re.search(r"(\d+)초", badge_aria)
                    min_val = int(match_min.group(1)) if match_min else 0
                    sec_val = int(match_sec.group(1)) if match_sec else 0
                    total_seconds = min_val * 60 + sec_val

                    if total_seconds < 60 or total_seconds >= 300: # 영상 길이 조건
                        continue

                    title_tag = await asyncio.to_thread(element.find_element, By.CSS_SELECTOR, "a#video-title-link")
                    title = await asyncio.to_thread(title_tag.get_attribute, "title")
                    href = await asyncio.to_thread(title_tag.get_attribute, "href")
                    link = f"[https://www.youtube.com](https://www.youtube.com){href}" if href.startswith("/watch") else href

                    thumbnail_anchor_tag = await asyncio.to_thread(element.find_element, By.CSS_SELECTOR, "a#thumbnail")
                    thumbnail_img_tag = await asyncio.to_thread(thumbnail_anchor_tag.find_element, By.CSS_SELECTOR, "yt-image > img")
                    thumbnail_url = await asyncio.to_thread(thumbnail_img_tag.get_attribute, "src")

                    if not thumbnail_url:
                        thumbnail_url = await asyncio.to_thread(thumbnail_img_tag.get_attribute, "data-src")
                    if not thumbnail_url:
                        video_id_match = re.search(r"v=([a-zA-Z0-9_-]{11})", link)
                        if video_id_match:
                            video_id = video_id_match.group(1)
                            thumbnail_url = f"[https://i.ytimg.com/vi/](https://i.ytimg.com/vi/){video_id}/maxresdefault.jpg"
                        else:
                            thumbnail_url = ""

                    if not title or title in self.seen_titles:
                        continue

                    self.seen_titles.add(title)
                    # 수집된 링크 정보를 큐에 비동기적으로 넣음
                    await self.link_queue.put({"title": title, "link": link, "thumbnail": thumbnail_url})
                    print(f"🔗 링크 수집: '{title}' ({len(self.seen_titles)}개)")

                except NoSuchElementException:
                    continue # 요소가 없으면 그냥 넘어감
                except Exception as e:
                    print(f"⚠️ 링크 수집 중 오류 발생: {e}")
                    continue

            # 스크롤 동작도 to_thread로 감쌈
            await asyncio.to_thread(self.driver.execute_script, "window.scrollTo(0, document.documentElement.scrollHeight);")
            await asyncio.sleep(scroll_pause) # 비동기적으로 대기
            
            new_height = await asyncio.to_thread(self.driver.execute_script, "return document.documentElement.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
            scroll_count += 1
        
        print(f"✅ 1차 영상 정보 수집 완료. 총 {len(self.seen_titles)}개.")
        # 링크 수집이 완료되었음을 알리는 신호 (옵션)
        for _ in range(5): # 여러 detail_scraper 코루틴이 종료될 수 있도록 마커를 여러 개 넣음
            await self.link_queue.put(None) 


    async def detail_scraper(self, worker_id):
        """
        큐에서 링크를 가져와 상세 페이지를 크롤링하고 전처리하는 코루틴.
        aiohttp + BeautifulSoup 또는 Selenium 중 선택하여 사용 가능.
        """
        while True:
            # 큐에서 비동기적으로 링크 정보 가져오기
            item = await self.link_queue.get()
            if item is None: # 종료 신호를 받으면 작업 종료
                self.link_queue.task_done()
                print(f"Worker {worker_id}: 링크 큐 종료 신호 수신, 종료합니다.")
                break

            print(f"🔍 [Worker {worker_id}] '{item['title']}' 영상 상세 정보 추출 및 전처리 중...")
            
            extracted_prompt = ""
            upload_time = ""

            try:
                # 방법 1: aiohttp + BeautifulSoup 사용 (추천: 빠르고 리소스 소모 적음)
                # 이 방법은 JavaScript 렌더링이 필요 없는 경우에 매우 효율적
                async with aiohttp.ClientSession() as session:
                    async with session.get(item["link"]) as response:
                        response.raise_for_status() # HTTP 오류 발생 시 예외 발생
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')

                        # uploadDate 추출
                        json_script = soup.find('script', {'type': 'application/ld+json'})
                        if json_script:
                            try:
                                json_data = json.loads(json_script.string)
                                upload_datetime_str = json_data.get("uploadDate", "")
                                upload_time = convert_upload_date_to_kst(upload_datetime_str)
                            except Exception as e:
                                print(f"⛔ [Worker {worker_id}] uploadDate 파싱 실패: {e}")
                        
                        # prompt 추출 (스크립트 태그 내 description 찾기 또는 직접 DOM 탐색)
                        # 유튜브는 동적으로 콘텐츠를 로드하므로, 이 방법만으로는 프롬프트 추출이 어려울 수 있습니다.
                        # Selenium이 필요하다면 아래 방법 2를 사용하세요.
                        # 여기서는 예시로 json_data에서 description을 찾거나, 일반적인 HTML 요소에서 찾으려 시도
                        # NOTE: YouTube의 description은 JS로 로드되는 경우가 많아 아래 코드로 안될 수 있습니다.
                        # 이 경우 Selenium을 계속 사용해야 합니다.
                        # extracted_prompt = soup.find('meta', {'name': 'description'})
                        # if extracted_prompt:
                        #     extracted_prompt = extracted_prompt.get('content')
                        # else: # Fallback to Selenium if direct fetch fails for prompt
                        #     extracted_prompt = await self._get_prompt_with_selenium(item['link'])
                        
                        # 유튜브의 description은 보통 특정 스크립트 태그 내에 있거나, 동적으로 로드됩니다.
                        # 따라서 BeautifulSoup만으로는 추출이 어려울 수 있어, Selenium을 사용하는 것이 더 확실합니다.

                        # 따라서, **프롬프트 추출은 Selenium에 의존하는 것이 현재로서는 가장 확실합니다.**
                        # 만약 aiohttp+BeautifulSoup으로 대부분의 정보를 얻고, 프롬프트만 Selenium으로 얻고 싶다면
                        # 아래와 같이 _get_prompt_with_selenium을 호출하도록 설계할 수 있습니다.
                        extracted_prompt = await self._get_prompt_with_selenium(item['link'])


            except aiohttp.ClientError as e:
                print(f"❌ [Worker {worker_id}] HTTP 요청 실패 ('{item['title']}'): {e}")
                self.link_queue.task_done()
                continue
            except Exception as e:
                print(f"❌ [Worker {worker_id}] 상세 정보 추출 중 일반 오류 발생 ('{item['title']}'): {e}")
                self.link_queue.task_done()
                continue

            if not extracted_prompt:
                print(f"⚠️ [Worker {worker_id}] '{item['title']}' ({item['link']}) 영상에서 프롬프트를 찾을 수 없습니다. 이 영상은 건너뜜니다.")
                self.link_queue.task_done()
                continue
            
            # 전처리된 제목과 프롬프트 추가
            cleaned_title_val = clean_title(item["title"])
            cleaned_prompt_val = clean_text_for_vectorization(extracted_prompt)

            # 최종 결과 큐에 데이터 넣기
            await self.result_queue.put({
                "title": item["title"],
                "prompt": extracted_prompt,
                "link": item["link"],
                "upload_time": upload_time,
                "thumbnail_link": item.get("thumbnail", ""),
                "cleaned_title": cleaned_title_val,
                "cleaned_prompt": cleaned_prompt_val
            })
            self.link_queue.task_done() # 큐에서 하나의 작업이 완료되었음을 알림

    async def _get_prompt_with_selenium(self, video_link):
        try:
            await asyncio.to_thread(self.driver.get, video_link)
            # await asyncio.sleep(2) # 페이지 로딩 대기
            
            # '더보기' 버튼 클릭 시도 (비동기적으로)
            try:
                expand_btn = await asyncio.to_thread(
                    WebDriverWait(self.driver, 5).until, # 5초 대기
                    EC.element_to_be_clickable((By.XPATH, '//*[@id="expand"]'))
                )
                await asyncio.to_thread(expand_btn.click)
                await asyncio.sleep(1) # 클릭 후 내용 로딩 대기
            except TimeoutException:
                pass # '더보기' 버튼이 없거나 클릭할 수 없으면 넘어감
            except NoSuchElementException:
                pass # '더보기' 버튼 자체가 없으면 넘어감

            # 프롬프트 추출 (비동기적으로)
            try:
                prompt_element = await asyncio.to_thread(
                    WebDriverWait(self.driver, 5).until,
                    EC.presence_of_element_located((By.XPATH, '//*[@id="description-inline-expander"]/yt-attributed-string/span/span[1]'))
                )
                return await asyncio.to_thread(lambda: prompt_element.text.strip())
            except TimeoutException:
                print(f"⚠️ '{video_link}' 영상에서 프롬프트 요소 찾기 시간 초과.")
                return ""
            except NoSuchElementException:
                print(f"⚠️ '{video_link}' 영상에서 프롬프트 요소를 찾을 수 없습니다.")
                return ""

        except Exception as e:
            print(f"❌ Selenium 프롬프트 추출 중 오류 발생 ('{video_link}'): {e}")
            return ""

    async def run_crawler(self):
        """크롤링 작업을 총괄하는 메인 코루틴"""
        await self._init_driver() # WebDriver 초기화
        
        start_time = time.time()
        
        # 링크 수집기와 상세 정보 추출기 코루틴 생성
        collector_task = asyncio.create_task(self.link_collector())
        
        # 여러 개의 detail_scraper worker를 동시에 실행 (예: 3개)
        num_workers = 3 
        scraper_tasks = [
            asyncio.create_task(self.detail_scraper(i + 1)) for i in range(num_workers)
        ]

        # 모든 링크가 수집될 때까지 기다림
        await collector_task
        
        # 링크 큐에 모든 작업이 처리될 때까지 기다림 (detail_scraper들이 모두 작업 마칠 때까지)
        await self.link_queue.join() 

        # 모든 detail_scraper worker 종료 신호 보내기 (None 마커가 처리될 때까지 대기)
        # 이미 link_collector에서 None을 충분히 넣었으므로, 이 부분은 불필요할 수 있음.
        # 하지만 명시적으로 모든 worker가 종료되도록 보장하는 것이 더 안전.
        for task in scraper_tasks:
            task.cancel() # 더 이상 처리할 링크가 없으므로 worker task를 취소

        # 결과 큐에서 모든 데이터를 수집
        all_crawled_data = []
        while not self.result_queue.empty():
            all_crawled_data.append(await self.result_queue.get())

        # ID 부여 및 정렬 (옵션: publishedAt 기준 정렬 후 ID 부여)
        # YouTube는 최신순으로 정렬되어 보여지므로, 수집된 순서대로 ID를 부여해도 무방할 수 있습니다.
        # 만약 publishedAt 기준으로 정확히 최신순으로 정렬하고 싶다면 아래 주석 해제
        # all_crawled_data.sort(key=lambda x: x['upload_time'], reverse=True) 
        
        final_results = []
        for i, data in enumerate(all_crawled_data):
            data['id'] = i + 1 # 1부터 시작하는 ID 부여
            final_results.append(data)

        df = pd.DataFrame(final_results)

        if not df.empty:
            df_to_save = df[["id", "title", "prompt", "link", "cleaned_title", "cleaned_prompt", "thumbnail_link", "upload_time"]]
            # df_to_save.to_csv("original_data.csv", index=False, encoding="utf-8-sig")
            # print("✅ 파일 저장 완료: original_data.csv")
            
            # JSON 파일 저장 추가
            json_file_path = "최적화_sample_data.json"
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(df_to_save.to_dict('records'), f, ensure_ascii=False, indent=4)
            print(f"✅ JSON 파일 저장 완료: {json_file_path}")

        else:
            print("ℹ️ 수집된 영상 정보가 없어 CSV/JSON 파일을 생성하지 않았습니다.")
            
        await self._close_driver() # WebDriver 종료

        end_time = time.time()
        total_time = end_time - start_time
        print(f"\n총 소요시간 : {total_time:.2f} 초")

        return df

async def main():
    crawler = YouTubeNewsCrawler(channel_url="https://www.youtube.com/@newskbs/videos", max_videos_to_collect=10, max_scrolls=5) # 테스트를 위해 수집 개수 및 스크롤 횟수 줄임
    crawled_data_df = await crawler.run_crawler()
    if not crawled_data_df.empty:
        print("\n--- 크롤링 결과 미리보기 (상위 5개) ---")
        print(crawled_data_df.head())
    else:
        print("\n--- 크롤링된 데이터가 없습니다. ---")

if __name__ == "__main__":
    asyncio.run(main())