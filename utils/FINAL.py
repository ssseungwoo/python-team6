import time
import json
import re
from datetime import datetime, timedelta
import pytz

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
from multiprocessing import Pool, cpu_count 

from langdetect import detect, DetectorFactory # 언어감지 알고리즘
DetectorFactory.seed = 0 # 난수 생성기의 시드값을 0으로 고정, langdetext 라이브러리가 언어를 감지 시에 일관되고 예측 가능한 결과 반환하도록 보장

BASE_YOUTUBE_URL = "https://www.youtube.com/"
CHANNEL_INFO = {
    "1": {"handle": "@ytnnews24", "filename": "YTN_VIDEO_DATA.json"},
    "2": {"handle": "@newskbs", "filename": "KBS_VIDEO_DATA.json"},
    "3": {"handle": "@sbsnews8", "filename": "SBS_VIDEO_DATA.json"}
}
SELECTED_CHANNEL_CODE = "2" # 변경 시 방송사 유튜브 채널 URL 변경
if SELECTED_CHANNEL_CODE in CHANNEL_INFO:
    YOUTUBE_MAIN_URL = f"{BASE_YOUTUBE_URL}{CHANNEL_INFO[SELECTED_CHANNEL_CODE]['handle']}/videos"
    OUTPUT_FILENAME = CHANNEL_INFO[SELECTED_CHANNEL_CODE]['filename']
    print(f"선택된 채널: {CHANNEL_INFO[SELECTED_CHANNEL_CODE]['handle']}, 크롤링 URL: {YOUTUBE_MAIN_URL}, 저장 파일명: {OUTPUT_FILENAME}")
else:
    print(f"오류: 유효하지 않은 채널 코드 '{SELECTED_CHANNEL_CODE}' 입니다. 기본값으로 설정하거나 프로그램을 종료합니다.")
    YOUTUBE_MAIN_URL = f"{BASE_YOUTUBE_URL}{CHANNEL_INFO['1']['handle']}/videos"
    OUTPUT_FILENAME = CHANNEL_INFO['1']['filename']

MIN_VIDEO_LENGTH_SECONDS = 60
MAX_VIDEO_LENGTH_SECONDS = 300
MAX_VIDEO_AGE_HOURS = 24 
SCROLL_PAUSE_TIME = 2
MAX_SCROLLS = 500
KST = pytz.timezone('Asia/Seoul')
filtered_video_urls_1st_stage = []
final_video_data = []

def clean_title(title): # title 전처리용
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


def clean_text_full(text): # description 전처리용
    if not isinstance(text, str):
        return ""
    idx_ytn_info = text.find('※ \'당신의 제보가 뉴스가 됩니다\'')
    if idx_ytn_info != -1:
        text = text[:idx_ytn_info]
    
    idx_sbs_info = text.find('☞더 자세한 정보')
    if idx_sbs_info != -1:
        text = text[:idx_sbs_info]

    match_news_end = re.search(r'(KBS\s*뉴스\s*[가-힣]{2,5}니다\.?)', text)
    if match_news_end:
        text = text[:match_news_end.start()] 
        text = re.sub(r'\[[^\]]+:\s*["“][^"”]+["”]\s*\]', ' ', text) 
        text = re.sub(r'[가-힣]{2,5}\s*기자(?:의)?\s*(?:보도(?:입니다)?|보돕니다)\.?', ' ', text) 
        text = re.sub(r'\[\s*(?:리포트|앵커|취재|기자|특파원|논평|해설)\s*\]', ' ', text)
        text = re.sub(r'\([^()]*\)', ' ', text) 
        text = re.sub(r'\{[^{}]*\}', ' ', text) 
        text = re.sub(r'\[[^\[\]]*\]', ' ', text) 
        text = re.sub(r"#[\w가-힣]+", " ", text) 
        text = re.sub(r'[^\w\s가-힣]', ' ', text) 
        text = re.sub(r'([가-힣ㅏ-ㅣ])\1{1,}', '', text) 
        text = re.sub(r'[ㅋㅎㅠㅜ]{2,}', '', text) 
        text = re.sub(r'\s+', ' ', text).strip() 
        
        return text.strip() 

    match_kbs_link = re.search(r'(KBS\s*기사\s*원문보기\s*[:：]\s*http[s]?://\S+)', text)
    if match_kbs_link:
        text = text[:match_kbs_link.start()] 
        text = re.sub(r'\[[^\]]+:\s*["“][^"”]+["”]\s*\]', ' ', text) 
        text = re.sub(r'[가-힣]{2,5}\s*기자(?:의)?\s*(?:보도(?:입니다)?|보돕니다)\.?', ' ', text) 
        text = re.sub(r'\[\s*(?:리포트|앵커|취재|기자|특파원|논평|해설)\s*\]', ' ', text)
        text = re.sub(r'\([^()]*\)', ' ', text) 
        text = re.sub(r'\{[^{}]*\}', ' ', text) 
        text = re.sub(r'\[[^\[\]]*\]', ' ', text) 
        text = re.sub(r"#[\w가-힣]+", " ", text) 
        text = re.sub(r'[^\w\s가-힣]', ' ', text) 
        text = re.sub(r'([가-힣ㅏ-ㅣ])\1{1,}', '', text) 
        text = re.sub(r'[ㅋㅎㅠㅜ]{2,}', '', text) 
        text = re.sub(r'\s+', ' ', text).strip() 
        
        return text.strip() 

    lines = text.splitlines()

    cleaned_lines = []
    common_junk_patterns = [
        # 저작권 문구: 'Copyright', 'ⓒ', 'All rights reserved', '무단 전재', '재배포', 'AI학습' 등
        r'^(?:Copyright\s*Ⓒ?\s*(?:KBS|SBS|YTN)\.?|ⓒ\s*(?:KBS|SBS|YTN))\s*\.?\s*All\s*rights\s*reserved\.?\s*(?:무단\s*전재(?:,)?\s*재배포\s*(?:및\s*(?:이용|AI학습\s*포함|AI학습\s*이용))?)?\s*(?:금지)?\s*$',
        # 제보/연락처/소셜 미디어 정보 (전화번호, 이메일, 카톡, 홈페이지 등)
        r'^\s*(?:홈페이지|애플리케이션|앱|카카오톡|페이스북|인스타그램|X|이메일|메일|문자|전화)[:：\s]*(?:[\'"]?(?:KBS|SBS|YTN)?\s*뉴스[\'"]?)?(?:.*(?:제보|친구\s*맺고\s*채팅|메시지\s*전송|@(?:sbs|kbs|ytn)\.co\.kr|누르고\s*\d+|.*-\d{3,4}-\d{4}|앱\s*설치|채널\s*추가|구독).*)?$',
        # 기자/제작진 정보: (영상취재:이상욱), 촬영기자:xxx/영상편집:yyy, 영상편집 : xxx 디자인 : zzz, 촬영 : xxx
        r'^\s*(?:영상취재|촬영기자|촬영|영상편집|편집|그래픽|디자인|화면제공|기자|앵커|특파원|기상캐스터|진행|리포터|논설위원|구성|제작|CG|자막뉴스)\s*[:：\|]?\s*(?:[^/\n]+(?:[/·]\s*[^/\n]+)*)?(?:\s*[:：]\s*.*)?$', # '|' 추가 및 뒤에 올 수 있는 이름 처리 강화
        # 기자 이름+이메일/소속: YTN 유투권 (r2kwon@ytn.co.kr)
        r'^\s*[A-Z]{2,4}\s*[가-힣]{2,4}\s*\([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\)\s*$',
        # 짧은 기자 이름만 있는 경우 (일반 텍스트와 겹칠 수 있으므로 주의)
        r'^\s*[가-힣]{2,4}\s*[기자앵커|특파원|기상캐스터]\s*$', # "이름 기자" 패턴 (단독 줄)
        # 홍보/안내 문구 시작: ☞, ▶, ♨, ▣, ※ 등의 특수문자 시작
        r'^(?:☞|▶|♨|#|▣|※)\s*(?:더\s*자세한\s*정보|트럼프\s*2기|기사\s*모아보기|지금\s*뜨거운\s*이슈|기사\s*원문|제보\s*하기|YTN\s*검색해\s*채널\s*추가|KBS제보\s*검색|당신의\s*제보가\s*뉴스가\s*됩니다|SBS뉴스\s*채널\s*구독|SBS뉴스\s*라이브|함께\s*토론하기|스프\s*구독|뉴스\s*채널\s*구독|채널\s*추가|클릭|보기|누르세요)?(?:.*(?:http[s]?://|n\.sbs\.co\.kr|news\.kbs\.co\.kr|ytn\.co\.kr|goo\.gl|premium\.sbs\.co\.kr|pf\.kakao\.com|www\.facebook\.com|www\.twitter\.com|www\.instagram\.com|ncd=\d+).*)?$', 
        # 일반적인 URL 단독 줄 (http/https 없는 경우도 포함)
        r'^\s*\b(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}(?:/[^\s]*)?\s*$',
        # 완벽한 URL 단독 줄
        r'^\s*http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+\s*$',
        # 해시태그 단독 줄 (쉼표로 구분된 여러 해시태그 포함)
        r'^\s*(?:#[\w가-힣\s]+(?:,\s*#[\w가-힣\s]+)*)\s*$',
    ]
    for line in lines:
        stripped_line = line.strip()
        is_junk = False
        for pattern in common_junk_patterns:
            if re.search(pattern, stripped_line, re.IGNORECASE):
                is_junk = True
                break
        if is_junk:
            continue
        cleaned_lines.append(stripped_line) 
    text = ' '.join(cleaned_lines) 

    # 줄 결합 후 후처리 단계에서 잔여 패턴 제거 (가장 중요)
    # YTN 기자/앵커의 끝맺음 문구 제거 강화
    text = re.sub(r'^(?:대담\s*발췌\s*:\s*이선\s*디지털뉴스팀\s*에디터|\s*지금까지\s*(?:[가-힣]{2,5}부에서\s*)?YTN\s*[가-힣]{2,5}입니다\.?)\s*', ' ', text, flags=re.MULTILINE)
    text = re.sub(r'YTN\s*[가-힣]{2,5}\s*입니다\.?', ' ', text) # "YTN 기자이름입니다."
    text = re.sub(r'\<앵커\>|\<기자\>', ' ', text) # <앵커>, <기자> 제거
    # 인터뷰 인용 구조 제거 ([인물명 직책 : "내용"])
    text = re.sub(r'\[[^\]]+:\s*["“][^"”]+["”]\s*\]', ' ', text)
    # 직업명 및 보도 관련 문구 제거 (황다예 기자의 보돕니다, [리포트] 등)
    text = re.sub(r'[가-힣]{2,5}\s*기자(?:의)?\s*(?:보도(?:입니다)?|보돕니다)\.?', ' ', text)
    text = re.sub(r'\[\s*(?:리포트|앵커|취재|기자|특파원|논평|해설)\s*\]', ' ', text)

    text = re.sub(
        r'(?:KBS|SBS|YTN)\s*(?:오톡|뉴스)?\s*(?:KBS제보|SBS뉴스|YTN 검색해)?\s*(?:검색)?\s*(?:전화)?\s*\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4}\s*(?:홈페이지)?\s*https?://\S+\s*(?:이메일)?\s*[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Z|a-z]{2,}\s*(?:Copyright\s*Ⓒ?\s*(?:KBS|SBS|YTN)\. All rights reserved\. 무단 전재, 재배포 및 이용\(AI 학습 포함\) 금지)?', 
        ' ', text, flags=re.IGNORECASE
    )
    # 기자/앵커의 끝맺음 문구 (다른 방송사나, 첫 번째 if 블록에서 처리되지 않은 경우 대비)
    text = re.sub(r'([가-힣]{2,7}(?:특파원|기자|앵커|리포터)?입니다\.?)', ' ', text)
    # 이름: 발언 형식 (정규식 강화: 콜론 앞뒤 공백 및 다양한 문자 허용)
    text = re.sub(r'[가-힣A-Z\s]{2,15}\s*[:：]\s*[가-힣\s]{2,200}', ' ', text)
    # 괄호 안 내용 제거 (이전에 줄 단위에서 제거되지 않은 경우를 대비)
    text = re.sub(r'\([^()]*\)', ' ', text)
    text = re.sub(r'\{[^{}]*\}', ' ', text)
    text = re.sub(r'\[[^\[\]]*\]', ' ', text) 
    # 해시태그 제거 (줄 단위에서 제거되지 않은 경우를 대비)
    text = re.sub(r"#[\w가-힣]+", " ", text)
    # 숫자 및 단위 제거 (한글 단위 포함)
    text = re.sub(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?[가-힣a-zA-Z%℃]+\b', ' ', text)
    text = re.sub(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b', ' ', text)
    text = re.sub(r'#\s*\d+', ' ', text) 
    # 따옴표 및 불필요한 기호 제거
    text = re.sub(r'["“”‘’\'`]', ' ', text)
    text = re.sub(r'([가-힣ㅏ-ㅣ])\1{1,}', '', text) 
    text = re.sub(r'[ㅋㅎㅠㅜ]{2,}', '', text) 
    text = re.sub(r"[^\w\s가-힣]", " ", text) 
    # 다중 공백을 단일 공백으로 변환하고 양 끝 공백 제거
    text = re.sub(r'\s+', ' ', text).strip()
    return text.strip()

def initialize_webdriver(): # WebDriver 초기화
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
        driver = webdriver.Chrome(service=service, options=chrome_options) # 
        print("WebDriver 초기화 완료.")
        return driver 
    except Exception as e:
        print(f"WebDriver 초기화 실패: {e}")
        return None

def parse_duration_to_seconds(duration_str):
    parts = list(map(int, duration_str.split(':')))
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    elif len(parts) == 2:
        return parts[0] * 60 + parts[1]
    return 0

def is_within_hours(upload_time_str, max_hours):
    upload_time_str = upload_time_str.replace(" ", "")

    if "분전" in upload_time_str:
        minutes = int(re.search(r'(\d+)', upload_time_str).group(1))
        return minutes <= 60 * max_hours
    elif "시간전" in upload_time_str:
        hours = int(re.search(r'(\d+)', upload_time_str).group(1))
        return hours <= max_hours
    elif "일전" in upload_time_str:
        days = int(re.search(r'(\d+)', upload_time_str).group(1))
        return days <= (max_hours // 24) 
    return False

def is_english_video(title, description):
    if not title and not description:
        return False
    title_is_english = False
    description_is_english = False
    try:
        if title and len(title.strip()) > 5: 
            if detect(title) == 'en':
                title_is_english = True
    except Exception:
        pass 
    try:
        if description and len(description.strip()) > 20: 
            if detect(description) == 'en':
                description_is_english = True
    except Exception:
        pass 
    return title_is_english and description_is_english

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
                print("3회 연속 스크롤 높이 변화 없음. 더 이상 로드할 컨텐츠가 없거나 로딩이 매우 느림. 중단")
                break
        else:
            no_change_count = 0
        last_height = new_height
 
        soup = BeautifulSoup(driver.page_source, 'html.parser') 
        video_containers = soup.select('ytd-rich-item-renderer, ytd-video-renderer') # 스크롤된 페이지에서 영상 리스트 가져옴

        if not video_containers:
            print("동영상 컨테이너 요소를 찾을 수 없음. 셀렉터를 확인")
            if no_change_count >= 3: break 
            continue # 스크롤 3회 이상 동안 변화 없으면 크롤링 중단
        current_scroll_videos_count = 0
        
        for video in video_containers: # 개별적 각 영상 조건 검사
            try:
                link_tag = video.select_one('a#thumbnail') 
                if not link_tag or 'href' not in link_tag.attrs: continue
                video_url_suffix = link_tag['href'] 
                if not video_url_suffix.startswith('/watch?v='): continue
                video_id = video_url_suffix.split('v=')[1].split('&')[0] 
                if video_id in seen_video_ids: continue # 유효하지 않은 링크, 이미 처리한 영상일 경우 건너뜀
                
                upload_time_element = video.select_one('div#metadata-line span.inline-metadata-item:last-of-type')
                upload_time_str = upload_time_element.get_text(strip=True) if upload_time_element else ""
                if not upload_time_str or "전" not in upload_time_str:  
                    continue # 업로드 시간 검사, 상대시간
                if "일 전" in upload_time_str: 
                    days_ago = int(re.search(r'(\d+)', upload_time_str).group(1))
                    if days_ago >= (MAX_VIDEO_AGE_HOURS // 24):
                        print("설정된 시간 초과영상발견, 1단계크롤링 종료")
                        stop_scrolling = True # 설정 조건 벗어나면 스크롤 중단
                        break
                
                duration_div = video.select_one('badge-shape.badge-shape-wiz--thumbnail-default div.badge-shape-wiz__text')
                if not duration_div:
                    continue
                duration_str = duration_div.get_text(strip=True)
                video_length_seconds = parse_duration_to_seconds(duration_str) 
                if not (MIN_VIDEO_LENGTH_SECONDS <= video_length_seconds <= MAX_VIDEO_LENGTH_SECONDS):
                    continue # 영상 길이 검사 60 ~ 300 초 사이 일때만 통과

                if video_url_suffix and upload_time_str: # 필터링된 영상 데이터 저장
                    full_url = f"{BASE_YOUTUBE_URL}{video_url_suffix}" 
                    filtered_video_urls_1st_stage.append({
                        "url": full_url,
                        "upload_time_summary": upload_time_str,
                    }) 
                    seen_video_ids.add(video_id)
                    current_scroll_videos_count += 1

            except (NoSuchElementException, StaleElementReferenceException, AttributeError, IndexError, TypeError) as e:
                continue
            except Exception as e:
                continue

        print(f"현재 스크롤에서 {current_scroll_videos_count}개의 유효 동영상 발견.")
        if stop_scrolling:
            break

        scroll_count += 1
        time.sleep(1)

    print(f"\n1단계 크롤링 완료. 총 {len(filtered_video_urls_1st_stage)}개의 동영상 후보가 수집됨.")
    unique_videos_dict = {video['url']: video for video in filtered_video_urls_1st_stage}
    filtered_video_urls_1st_stage[:] = list(unique_videos_dict.values())
    print(f"최종 1단계 필터링 후 {len(filtered_video_urls_1st_stage)}개의 동영상 후보.")

def process_video_data(video_data_1st_stage):
    ydl_opts = {
        'noplaylist': True,
        'quiet': True,
        'no_warnings': True,
        'forcethumbnail': True,
        'skip_download': True,
    }
    video_url = video_data_1st_stage['url']
    print(f"워커: '{video_data_1st_stage.get('upload_time_summary', 'N/A')}' 동영상 상세 정보 크롤링 및 전처리 중: {video_url}")
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl: # yt-dlp 로 메타데이터 수집
            video_info = ydl.extract_info(video_url, download=False)

        title = video_info.get('title', '제목 없음') # 영상 제목 추출
        description = video_info.get('description', '설명 없음') # 영상 프롬포트 추출
        webpage_url = video_info.get('webpage_url', video_url) # 영상 웹 페이지 URL 추출
        thumbnail = video_info.get('thumbnail', None) # 썸네일 링크 URL 추출
        if 'thumbnails' in video_info and video_info['thumbnails']: # 여러 개의 썸네일 저장 목록 존재시
            thumbnail = video_info['thumbnails'][-1].get('url', thumbnail) # 가장 마지막(고화질)썸네일 추출
        view_count = video_info.get('view_count') # 조회수 추출

        upload_date_kst_formatted = None # KST 업로드 날짜 초기화
        if 'timestamp' in video_info and video_info['timestamp'] is not None:
            dt_object_utc = datetime.fromtimestamp(video_info['timestamp'], tz=pytz.utc) # datetime 객체로 변환
            kst_timezone = pytz.timezone('Asia/Seoul') 
            dt_object_kst = dt_object_utc.astimezone(kst_timezone) # 한국표준시로 변환
            upload_date_kst_formatted = dt_object_kst.strftime('%Y-%m-%d %H:%M:%S') # 포맷팅

        now_kst = datetime.now(KST) # 작업 현재 시간
        if upload_date_kst_formatted: # KST 시간 유효 시
            try:
                uploaded_dt_kst = KST.localize(datetime.strptime(upload_date_kst_formatted, '%Y-%m-%d %H:%M:%S'))
                time_difference = now_kst - uploaded_dt_kst # 현재 시간, 업로드 시간 정확한 비교
                if time_difference.total_seconds() >= MAX_VIDEO_AGE_HOURS * 3600: # 시간 초과시
                    print(f" -> '{upload_date_kst_formatted}'(KST) 영상은 {MAX_VIDEO_AGE_HOURS}시간 초과로 제외")
                    return None # 해당 영상 데이터 제거
            except ValueError:
                print(f" -> KST 업로드 시간 '{upload_date_kst_formatted}' 파싱 오류 필터링 않고 포함")
        else:
            print(f" -> YT-DLP에서 KST 업로드 시간 파싱 불가 필터링 않고 포함")

        if is_english_video(title, description):
            print(f" -> 영문 영상 제외: '{title}'")
            return None

        cleaned_title_result = clean_title(title)
        description = clean_text_full(description) 

        return {
            "title": title,
            "cleaned_title": cleaned_title_result,
            "upload_date_kst": upload_date_kst_formatted,
            "description": description, # 이제 이 description은 전처리된 내용입니다.
            "video_link": webpage_url,
            "thumbnail_link": thumbnail,
            "view_count": view_count 
        }

    except yt_dlp.utils.DownloadError as e:
        print(f"yt-dlp 오류 발생 (URL: {video_url}): {e}")
        return None
    except Exception as e:
        print(f"yt-dlp 처리 중 예상치 못한 오류 발생 (URL: {video_url}): {e}")
        return None


if __name__ == "__main__":
    driver = initialize_webdriver() # WebDriver 초기화 함수 호출

    if driver: # Webdriver 초기화 성공 시
        crawl_main_page_and_filter_videos(driver) # 1단계 메인 페이지 크롤링, 1차 필터링
        driver.quit() # WebDriver 종료
        print("WebDriver 종료")
    else:
        print("WebDriver가 초기화되지 않아 1단계 크롤링을 실행할 수 없음. 프로그램 종료")
        exit()

    print("\n2단계 크롤링 및 전처리 시작(병렬 처리)")
    total_start_time = time.time() 

    if filtered_video_urls_1st_stage: # 1단계 필터링 영상 URL 목록 존재 시
        num_cores = cpu_count() # 시스템 CPU 코어 수 확인
        num_workers = num_cores # 워커 수 설정
        print(f"시스템 코어 수: {num_cores}, 설정된 워커 수: {num_workers}")

        temp_final_data_before_id = [] # 최종 데이터 저장 위한 임시 리스트 초기화

        with Pool(processes=num_workers) as pool:  # 지정된 수의 워커로 프로세스 풀 생성 
            results = pool.map(process_video_data, filtered_video_urls_1st_stage)
            # 각 URL에 대해 병렬로 process_video_data 함수 실행

        for detailed_info in results: # 병렬 처리 결과 반복
            if detailed_info: 
                temp_final_data_before_id.append(detailed_info)

        print(f"\n총 {len(temp_final_data_before_id)}개의 유효한 영상 상세 정보 및 전처리 완료.")

        if temp_final_data_before_id: # 유효한 영상 데이터 존재 시
            temp_final_data_before_id.sort( # 영상 데이터 정렬
                key=lambda x: datetime.strptime(x['upload_date_kst'], '%Y-%m-%d %H:%M:%S') 
                if x.get('upload_date_kst') else datetime.min, # 값이 없으면 datetime.min으로 과거로간주
                reverse=True # 최신순 정렬
            ) # upload_date_kst 값 존재 시 datetime 객체로 바꿔서 비교,
            for i, item in enumerate(temp_final_data_before_id):
                item['id'] = i + 1 # 고유 id 부여
                final_video_data.append(item) # 최종 데이터 리스트에 추가
        try:
            with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f: # 지정된 파일명으로 쓰기 모드
                output_data = [ # 저장될 데이터 형식 정의
                    {
                        "id": item.get("id"),
                        "title": item.get("title"),
                        "cleaned_title": item.get("cleaned_title"),
                        "upload_date_kst": item.get("upload_date_kst"),
                        "description": item.get("description"),
                        "video_link": item.get("video_link"),
                        "thumbnail_link": item.get("thumbnail_link"),
                        "view_count": item.get("view_count") 
                    } for item in final_video_data
                ]
                json.dump(output_data, f, ensure_ascii=False, indent=4) # JSON 형식으로 파일에 저장
            print(f"크롤링된 데이터가 '{OUTPUT_FILENAME}' 파일에 성공적으로 저장됨")
        except Exception as e:
            print(f"JSON 파일 저장 중 오류 발생: {e}")
    else:
        print("1단계 필터링 영상 없음 2단계 크롤링을 진행 못함")

    total_end_time = time.time()
    total_elapsed_time = total_end_time - total_start_time
    print(f"\n**전체 크롤링 작업 소요 시간**: {total_elapsed_time:.4f} 초")