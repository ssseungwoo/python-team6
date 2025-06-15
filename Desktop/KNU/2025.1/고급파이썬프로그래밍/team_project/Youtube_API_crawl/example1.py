# 비동기 프로그래밍 적용
# 최적화 단계 

import time
import json
import re
from datetime import datetime, timedelta
import pytz
import asyncio # 비동기 처리를 위한 모듈
from concurrent.futures import ThreadPoolExecutor # 비동기 내에서 블로킹 작업을 처리하기 위함

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

from bs4 import BeautifulSoup
import yt_dlp
from multiprocessing import Pool, cpu_count # 병렬 처리를 위한 모듈

# Okt 객체는 프로세스마다 생성될 것이므로, 글로벌 변수에서 제거
# okt = Okt()

YOUTUBE_MAIN_URL = "https://www.youtube.com/channel/UCcQTRi69dsVYHN3exePtZ1A/videos/videos"
SCROLL_PAUSE_TIME = 2
MAX_SCROLLS = 500

MIN_VIDEO_LENGTH_SECONDS = 60
MAX_VIDEO_LENGTH_SECONDS = 300
KST = pytz.timezone('Asia/Seoul')

filtered_video_urls_1st_stage = []
final_video_data = []

# --- 전처리 함수들 (변동 없음) ---
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
    text = re.sub(r"^(▣|◇|■|▲|▼|◆|○|●|△|▷|▶|※|◎)\s*(KBS|SBS|YTN)\s*기사 원문보기\s*:\s*.*", "", text, flags=re.DOTALL)
    text = re.sub(r"무단\s*배포\s*이용\s*금지", "", text, flags=re.IGNORECASE)
    text = re.sub(r"무단\s*전재,\s*재배포\s*및\s*이용(?:\(AI\s*학습\s*포함\))?\s*금지", "", text, flags=re.IGNORECASE)
    text = re.sub(r"Copyright\s*©.*(?:All rights reserved\..*)?", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"Copyright\s*©.*", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"#[\w가-힣]+", "", text)
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

# Okt 객체를 사용하는 함수는 프로세스 내에서 생성되도록 수정
# 비동기 함수 내에서 executor를 통해 호출될 예정
def _clean_text_for_vectorization_sync(text, _okt_instance, _korean_stopwords):
    if not isinstance(text, str):
        return ""
    cleaned_initial_text = clean_text_full(text)
    cleaned_initial_text = re.sub(r'([가-힣ㅏ-ㅣ])\1{1,}', '', cleaned_initial_text)
    cleaned_initial_text = re.sub(r'[ㅋㅎㅠㅜ]{2,}', '', cleaned_initial_text)
    cleaned_initial_text = re.sub(r'[^\w\s가-힣.]', ' ', cleaned_initial_text)
    cleaned_initial_text = re.sub(r'\s+', ' ', cleaned_initial_text).strip()

    words = []
    for word, tag in _okt_instance.pos(cleaned_initial_text, norm=True, stem=True):
        if tag in ['Noun', 'Verb', 'Adjective', 'Adverb']:
            words.append(word)

    words = [word for word in words if word not in _korean_stopwords and len(word) > 1]
    return ' '.join(words)


# --- WebDriver 초기화 함수 (변동 없음) ---
def initialize_webdriver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--incognito")
    try:
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        print("WebDriver 초기화 완료.")
        return driver
    except Exception as e:
        print(f"WebDriver 초기화 실패: {e}")
        return None

# --- 시간 파싱 도우미 함수 (변동 없음) ---
def parse_duration_to_seconds(duration_str):
    parts = list(map(int, duration_str.split(':')))
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    elif len(parts) == 2:
        return parts[0] * 60 + parts[1]
    return 0

def is_within_24_hours(upload_time_str):
    upload_time_str = upload_time_str.replace(" ", "")
    if "분전" in upload_time_str:
        minutes = int(re.search(r'(\d+)', upload_time_str).group(1))
        return minutes <= 60 * 24
    elif "시간전" in upload_time_str:
        hours = int(re.search(r'(\d+)', upload_time_str).group(1))
        return hours <= 24
    elif "일전" in upload_time_str:
        days = int(re.search(r'(\d+)', upload_time_str).group(1))
        return days < 1 or (days == 1 and "시간전" not in upload_time_str)
    return False

# --- 1단계 - 유튜브 메인 페이지 동영상 목록 크롤링 및 빠른 필터링 (변동 없음) ---
def crawl_main_page_and_filter_videos(driver):
    print("1단계 크롤링 시작: 메인 페이지 동영상 목록 필터링")
    try:
        driver.get(YOUTUBE_MAIN_URL)
        print(f"{YOUTUBE_MAIN_URL} 접속 완료.")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "contents"))
        )
        print("페이지 컨텐츠 로딩 완료.")
    except TimeoutException:
        print("페이지 컨텐츠 로딩 시간 초과. 네트워크 상태를 확인하거나 타임아웃을 늘리세요.")
        return
    except WebDriverException as e:
        print(f"유튜브 메인 페이지 접속 중 오류 발생: {e}")
        return

    seen_video_ids = set()
    scroll_count = 0
    last_height = driver.execute_script("return document.documentElement.scrollHeight")
    stop_scrolling = False
    no_change_count = 0

    while not stop_scrolling and scroll_count < MAX_SCROLLS:
        print(f"\n스크롤 {scroll_count + 1}회 시작...")
        driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME + 1)

        new_height = driver.execute_script("return document.documentElement.scrollHeight")

        if new_height == last_height:
            no_change_count += 1
            print(f"높이 변화 없음 (연속 {no_change_count}회).")
            if no_change_count >= 3:
                print("3회 연속 스크롤 높이 변화 없음. 더 이상 로드할 컨텐츠가 없거나 로딩이 매우 느립니다. 중단합니다.")
                break
        else:
            no_change_count = 0

        last_height = new_height

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        video_containers = soup.select('ytd-rich-item-renderer, ytd-video-renderer')
        if not video_containers:
            print("경고: 동영상 컨테이너 요소를 찾을 수 없습니다. 셀렉터를 확인하세요.")
            if no_change_count >= 3:
                break
            continue

        current_scroll_videos_count = 0
        for video in video_containers:
            try:
                link_tag = video.select_one('a#thumbnail')
                if not link_tag or 'href' not in link_tag.attrs:
                    continue
                video_url_suffix = link_tag['href']
                if not video_url_suffix.startswith('/watch?v='):
                    continue
                video_id = video_url_suffix.split('v=')[1].split('&')[0]
                if video_id in seen_video_ids:
                    continue

                upload_time_element = video.select_one('div#metadata-line span.inline-metadata-item:last-of-type')
                upload_time_str = upload_time_element.get_text(strip=True) if upload_time_element else ""
                if not upload_time_str or "전" not in upload_time_str:
                    continue

                # 1단계에서 채널명으로 필터링 추가 (선택 사항, 필요 시 주석 해제하여 사용)
                # channel_name_element = video.select_one('a#channel-name yt-formatted-string') # 실제 셀렉터 확인 필요
                # channel_name = channel_name_element.get_text(strip=True) if channel_name_element else ""
                # trusted_channels = ["KBS 뉴스", "SBS 뉴스", "YTN 뉴스"] # 예시 채널명
                # if channel_name and channel_name not in trusted_channels:
                #    continue # 신뢰할 수 없는 채널이면 스킵
                # else:
                #    print(f" -> 채널명: {channel_name}") # 디버깅용

                if "일 전" in upload_time_str:
                    days_ago = int(re.search(r'(\d+)', upload_time_str).group(1))
                    if days_ago >= 1:
                        print(f"'{upload_time_str}' 동영상 발견. 1단계 크롤링을 중단합니다.")
                        stop_scrolling = True
                        break
                if not is_within_24_hours(upload_time_str):
                    continue
                duration_div = video.select_one('badge-shape.badge-shape-wiz--thumbnail-default div.badge-shape-wiz__text')
                if not duration_div:
                    continue
                duration_str = duration_div.get_text(strip=True)
                video_length_seconds = parse_duration_to_seconds(duration_str)

                if not (MIN_VIDEO_LENGTH_SECONDS <= video_length_seconds <= MAX_VIDEO_LENGTH_SECONDS):
                    continue

                if video_url_suffix and upload_time_str:
                    full_url = f"https://www.youtube.com{video_url_suffix}" # YOUTUBE_MAIN_URL을 직접 사용하지 않고 https://www.youtube.com/으로 변경
                    filtered_video_urls_1st_stage.append({
                        "url": full_url,
                        "upload_time_summary": upload_time_str,
                    })
                    seen_video_ids.add(video_id)
                    current_scroll_videos_count += 1

            except (NoSuchElementException, StaleElementReferenceException, AttributeError, IndexError, TypeError) as e:
                # print(f"1단계 동영상 파싱 오류 (일반적 오류): {e}") # 디버깅 시에만
                continue
            except Exception as e:
                print(f"1단계 처리 중 예상치 못한 오류 발생: {e}")
                continue

        print(f"현재 스크롤에서 {current_scroll_videos_count}개의 유효한 동영상 후보 발견.")
        if stop_scrolling:
            break

        scroll_count += 1
        time.sleep(1)

    print(f"\n1단계 크롤링 완료. 총 {len(filtered_video_urls_1st_stage)}개의 동영상 후보가 수집되었습니다.")
    unique_videos_dict = {video['url']: video for video in filtered_video_urls_1st_stage}
    filtered_video_urls_1st_stage[:] = list(unique_videos_dict.values())
    print(f"최종 1단계 필터링 후 {len(filtered_video_urls_1st_stage)}개의 유니크한 동영상 후보.")


# --- 비동기 크롤링 및 전처리 함수 (각 워커 프로세스에서 실행) ---
async def async_process_video_data(video_data_1st_stage, executor, kst_timezone, now_kst):
    from konlpy.tag import Okt # 각 프로세스/스레드에서 Okt 객체 생성
    _okt = Okt()

    korean_stopwords = [
        '오늘', '이번', '지난', '되다', '하다', '있다', '이다', '것', '수', '그', '더', '좀', '잘',
        '가장', '다', '또', '많이', '그리고', '그러나', '하지만', '따라', '등', '등등', '통해',
        '까지', '부터', '대한', '으로', '에서', '에게', '에게서', '보다', '때문',
        '습니다', '합니다', '합니다만', '입니다만', '이라고', '이었습니다', '였습니다',
        '년', '월', '일', '시', '분', '초', '오전', '오후', '이번주', '지난주', '다음주', '이달', '지난달', '다음달', '올해', '지난해', '내년',
        '명', '원', '씨', '보시', '하시', '들어보시', '확인해보시', '시키', '알아보시', '해보시',
        '어서', '으니', '으면', '어도', '으니', '으러',
    ]

    ydl_opts = {
        'noplaylist': True,
        'quiet': True,
        'no_warnings': True,
        'forcethumbnail': True,
        'skip_download': True,
    }

    video_url = video_data_1st_stage['url']
    # print(f"워커: '{video_data_1st_stage.get('upload_time_summary', 'N/A')}' 동영상 상세 정보 크롤링 및 전처리 중: {video_url}")

    try:
        # yt-dlp 호출은 블로킹 작업이므로 executor에서 실행
        # (yt-dlp는 내부적으로도 네트워크 I/O를 수행하므로, CPU 바운드라기보다는 I/O 바운드 작업으로 볼 수 있음)
        video_info = await asyncio.get_event_loop().run_in_executor(
            executor,
            lambda: yt_dlp.YoutubeDL(ydl_opts).extract_info(video_url, download=False)
        )

        title = video_info.get('title', '제목 없음')
        description = video_info.get('description', '설명 없음')
        webpage_url = video_info.get('webpage_url', video_url)
        thumbnail = video_info.get('thumbnail', None)
        if 'thumbnails' in video_info and video_info['thumbnails']:
            thumbnail = video_info['thumbnails'][-1].get('url', thumbnail)

        upload_date_kst_formatted = None
        if 'timestamp' in video_info and video_info['timestamp'] is not None:
            dt_object_utc = datetime.fromtimestamp(video_info['timestamp'], tz=pytz.utc)
            dt_object_kst = dt_object_utc.astimezone(kst_timezone)
            upload_date_kst_formatted = dt_object_kst.strftime('%Y-%m-%d %H:%M:%S')

        # 24시간 필터링
        if upload_date_kst_formatted:
            try:
                uploaded_dt_kst = kst_timezone.localize(datetime.strptime(upload_date_kst_formatted, '%Y-%m-%d %H:%M:%S'))
                time_difference = now_kst - uploaded_dt_kst
                if time_difference.total_seconds() >= 24 * 3600:
                    # print(f" -> '{upload_date_kst_formatted}' (KST) 영상은 24시간을 초과하여 제외합니다.")
                    return None
            except ValueError:
                print(f" -> 경고: KST 업로드 시간 '{upload_date_kst_formatted}' 파싱 오류. 필터링하지 않고 포함합니다.")
        else:
            print(f" -> 경고: YT-DLP에서 KST 업로드 시간을 가져오지 못했습니다. 이 영상은 필터링하지 않고 포함합니다.")

        # 전처리 함수 적용 (CPU 바운드 작업이므로 executor에서 실행)
        cleaned_title_result = clean_title(title) # 이 함수는 간단하여 executor에 넘길 필요 없음
        cleaned_description_result = await asyncio.get_event_loop().run_in_executor(
            executor,
            _clean_text_for_vectorization_sync,
            description, _okt, korean_stopwords # _okt와 불용어 리스트를 인자로 넘김
        )

        # print(f" -> 제목: {title[:30]}..., 업로드: {upload_date_kst_formatted}") # 너무 많은 출력 방지

        return {
            "title": title,
            "cleaned_title": cleaned_title_result,
            "upload_date_kst": upload_date_kst_formatted,
            "description": description,
            "cleaned_description": cleaned_description_result,
            "video_link": webpage_url,
            "thumbnail_link": thumbnail
        }

    except yt_dlp.utils.DownloadError as e:
        print(f"워커: yt-dlp 오류 발생 (URL: {video_url}): {e}")
        return None
    except Exception as e:
        print(f"워커: 처리 중 예상치 못한 오류 발생 (URL: {video_url}): {e}")
        return None

# 각 워커 프로세스가 실행할 함수 (비동기 함수들을 묶어 실행)
def run_async_tasks_in_worker(video_data_list_for_worker):
    from konlpy.tag import Okt # 각 프로세스 시작 시 Okt 객체 생성
    _okt = Okt() # 이 위치에서 Okt 객체를 생성해야 각 워커 프로세스가 자신만의 Okt 인스턴스를 가집니다.

    korean_stopwords = [
        '오늘', '이번', '지난', '되다', '하다', '있다', '이다', '것', '수', '그', '더', '좀', '잘',
        '가장', '다', '또', '많이', '그리고', '그러나', '하지만', '따라', '등', '등등', '통해',
        '까지', '부터', '대한', '으로', '에서', '에게', '에게서', '보다', '때문',
        '습니다', '합니다', '합니다만', '입니다만', '이라고', '이었습니다', '였습니다',
        '년', '월', '일', '시', '분', '초', '오전', '오후', '이번주', '지난주', '다음주', '이달', '지난달', '다음달', '올해', '지난해', '내년',
        '명', '원', '씨', '보시', '하시', '들어보시', '확인해보시', '시키', '알아보시', '해보시',
        '어서', '으니', '으면', '어도', '으니', '으러',
    ]

    executor = ThreadPoolExecutor(max_workers=cpu_count() * 2)

    kst_timezone = pytz.timezone('Asia/Seoul')
    now_kst = datetime.now(kst_timezone)

    async def worker_main_async_task():
        tasks = [
            async_process_video_data(video_data, executor, kst_timezone, now_kst, _okt, korean_stopwords)
            for video_data in video_data_list_for_worker
        ]
        return await asyncio.gather(*tasks)

    results = asyncio.run(worker_main_async_task())
    executor.shutdown(wait=True)
    return [res for res in results if res is not None]


# --- 메인 실행 ---
if __name__ == "__main__":
    # Windows에서는 multiprocessing 시작 메서드를 'spawn'으로 명시적으로 설정해야 합니다.
    # macOS/Linux는 기본적으로 'fork'를 사용하지만, 일관성을 위해 설정하는 것이 좋습니다.
    # 특히 Jupyter/IPython 환경에서는 'spawn'이 필수적입니다.
    # if platform.system() == "Windows":
    #    multiprocessing.freeze_support() # 윈도우에서 실행 가능하게 함 (선택 사항)
    #    multiprocessing.set_start_method('spawn', force=True) # 필요 시 주석 해제

    # 1단계 크롤링 (단일 프로세스)
    driver = initialize_webdriver()
    if driver:
        crawl_main_page_and_filter_videos(driver)
        driver.quit()
        print("WebDriver 종료.")
    else:
        print("WebDriver 초기화 실패. 1단계 크롤링을 실행할 수 없습니다. 프로그램을 종료합니다.")
        exit()

    print("\n2단계 크롤링 및 전처리 시작 (비동기+병렬 복합 처리)...")
    total_start_time = time.time()

    if filtered_video_urls_1st_stage:
        num_cores = cpu_count()
        # 워커 수 결정: 코어 수와 동일하게 시작하고, I/O 바운드 특성을 고려하여 더 늘려볼 수 있음
        num_workers = num_cores # 예: 4코어 노트북의 경우 4개 워커
        print(f"시스템 코어 수: {num_cores}, 설정된 병렬 워커 수: {num_workers}")

        # 전체 데이터를 워커 수만큼 청크로 나눔
        # 각 워커가 처리할 비디오 데이터 리스트를 만듭니다.
        # 이렇게 하면 Pool.map에 단일 리스트를 넘겨줄 수 있습니다.
        chunk_size = (len(filtered_video_urls_1st_stage) + num_workers - 1) // num_workers
        chunks = [
            filtered_video_urls_1st_stage[i:i + chunk_size]
            for i in range(0, len(filtered_video_urls_1st_stage), chunk_size)
        ]

        temp_final_data_before_id = []

        with Pool(processes=num_workers) as pool:
            # pool.map은 각 청크를 run_async_tasks_in_worker 함수에 전달합니다.
            # 각 워커는 할당된 청크 내에서 비동기적으로 작업을 처리합니다.
            nested_results = pool.map(run_async_tasks_in_worker, chunks)

        # 중첩된 결과를 하나의 리스트로 평탄화
        for worker_results in nested_results:
            temp_final_data_before_id.extend(worker_results)

        print(f"\n총 {len(temp_final_data_before_id)}개의 유효한 영상 상세 정보 및 전처리 완료.")

        # 최종 데이터 정렬 및 ID 부여 (변동 없음)
        if temp_final_data_before_id:
            temp_final_data_before_id.sort(
                key=lambda x: datetime.strptime(x['upload_date_kst'], '%Y-%m-%d %H:%M:%S') if x.get('upload_date_kst') else datetime.min,
                reverse=True
            )
            for i, item in enumerate(temp_final_data_before_id):
                item['id'] = i + 1
                final_video_data.append(item)

        # JSON 파일 저장 (변동 없음)
        output_filename = "비동기_병렬처리_예제_데이터.json"
        try:
            with open(output_filename, 'w', encoding='utf-8') as f:
                output_data = [
                    {
                        "id": item.get("id"),
                        "title": item.get("title"),
                        "cleaned_title": item.get("cleaned_title"),
                        "upload_date_kst": item.get("upload_date_kst"),
                        "description": item.get("description"),
                        "cleaned_description": item.get("cleaned_description"),
                        "video_link": item.get("video_link"),
                        "thumbnail_link": item.get("thumbnail_link")
                    } for item in final_video_data
                ]
                json.dump(output_data, f, ensure_ascii=False, indent=4)
            print(f"크롤링된 데이터가 '{output_filename}' 파일에 성공적으로 저장되었습니다.")
        except Exception as e:
            print(f"JSON 파일 저장 중 오류 발생: {e}")
    else:
        print("1단계 필터링된 영상이 없어 2단계 크롤링을 진행하지 않습니다.")

    total_end_time = time.time()
    total_elapsed_time = total_end_time - total_start_time
    print(f"\n**전체 크롤링 작업 소요 시간**: {total_elapsed_time:.4f} 초")