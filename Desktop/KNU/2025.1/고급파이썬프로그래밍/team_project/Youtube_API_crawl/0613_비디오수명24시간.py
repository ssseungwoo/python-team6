import time
import json
import re
from datetime import datetime, timedelta
import pytz
from konlpy.tag import Okt

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
from multiprocessing import Pool, cpu_count # 병렬 처리를 위한 모듈 추가

from langdetect import detect, DetectorFactory # Add langdetect library for language detection
DetectorFactory.seed = 0 # For reproducibility of language detection

# 전역 변수는 그대로 유지
okt = Okt()

# 이 부분 변경 절대 하지말것
# YOUTUBE_MAIN_URL = "https://www.youtube.com/@ytnnews24/videos" 
YOUTUBE_MAIN_URL = "https://www.youtube.com/@newskbs/videos"
# YOUTUBE_MAIN_URL = "https://www.youtube.com/@sbsnews8/videos"
# 이 부분 변경 절대 하지말것

SCROLL_PAUSE_TIME = 2
MAX_SCROLLS = 500

MIN_VIDEO_LENGTH_SECONDS = 60
MAX_VIDEO_LENGTH_SECONDS = 300
MAX_VIDEO_AGE_HOURS = 24 # 96시간에서 24시간으로 변경

KST = pytz.timezone('Asia/Seoul')

filtered_video_urls_1st_stage = []
final_video_data = []

def clean_title(title): # title 전처리용
    if not isinstance(title, str):
        return ""

    title = re.sub(r"\s*/\s*(KBS|SBS|YTN).*", "", title) # 예: "/KBS"와 같은 방송사 정보 제거
    title = re.sub(r"\([^)]*\)", "", title) # 소괄호 안 내용 제거
    title = re.sub(r"\[[^\]]*\]", "", title) # 대괄호 안 내용 제거
    title = re.sub(r"\{[^}]*\}", "", title) # 중괄호 안 내용 제거
    title = re.sub(r"【[^】]*】", "", title) # 특수 괄호 안 내용 제거
    title = re.sub(r'([가-힣ㅏ-ㅣ])\1{1,}', '', title) # 자음/모음 반복 제거 (예: ㅋㅋㅋㅋ -> )
    title = re.sub(r'[ㅋㅎㅠㅜ]{2,}', '', title) # 감탄사 자음/모음 반복 제거
    title = re.sub(r'앗|헉|윽|흥|풉|에구|읏|으음|아악|끼야|푸하하|하하하|히히히|헤헤헤|흐흐흐|낄낄|깔깔|콜록콜록|훌쩍|쉿', '', title) # 감탄사 제거
    title = re.sub(r"[^\w\s가-힣.%]", " ", title) # 알파벳, 숫자, 한글, 공백, . % 외 문자 제거
    title = re.sub(r"[·•]{3,}|\.{3,}|…", " ", title) # 연속된 점 또는 특수 점 제거
    title = re.sub(r"\s+", " ", title) # 다중 공백을 단일 공백으로 변환

    return title.strip() # 양 끝 공백 제거


def clean_text_full(text): # description 전처리용
    if not isinstance(text, str):
        return ""

    # 1. 초기 줄별 필터링 (가장 먼저 적용)
    lines = text.splitlines()
    cleaned_lines = []
    
    # 통합된 제보/안내/Copyright/저작권 패턴 (방송사 무관)
    # 특정 키워드와 숫자/URL/특수문자 조합을 포괄적으로 처리
    common_junk_patterns = [
        r'(?:영상취재|촬영기자|영상편집|그래픽|디자인|화면제공|기자|앵커|특파원|기상캐스터|진행|리포터|논설위원|구성|제작)\s*[:：]\s*.*', # 기자/제작진 정보
        r'([가-힣]{2,4}\s*)[기자앵커|특파원|기상캐스터]', # "이름 기자" 패턴
        r'\b(?:[KBS|SBS|YTN]\s*)?(?:뉴스\s*)?(?:채널\s*구독|뉴스\s*라이브|모바일\s*24|지금\s*뜨거운\s*이슈|함께\s*토론하기|스프\s*구독|기사\s*모아보기|실시간으로\s*만나\s*보세요|기사\s*원문보기|제보\s*하기|당신의\s*제보가\s*뉴스가\s*됩니다)', # 홍보/안내 문구
        r'(?:홈페이지|애플리케이션|카카오톡|페이스북|이메일|문자|전화)[:\s]*(?:[\'"]?(?:KBS|SBS|YTN)\s*뉴스[\'"]?)?(?:.*제보|.*친구\s*맺고\s*채팅|.*메시지\s*전송|.*@sbs\.co\.kr|.*@kbs\.co\.kr|.*@ytn\.co\.kr|.*누르고\s*\d+|.*\d{2}-\d{3,4}-\d{4}|.*앱\s*설치|.*채널\s*추가)', # 제보 안내
        r'(?:☞|▶|♨|#)\s*(?:더\s*자세한\s*정보|트럼프\s*2기|기사\s*모아보기|지금\s*뜨거운\s*이슈|기사\s*원문|제보\s*하기|YTN\s*검색해\s*채널\s*추가)?(?:.*(?:http[s]?://|n\.sbs\.co\.kr|news\.kbs\.co\.kr|ytn\.co\.kr).*)?', # 링크/홍보/해시태그 줄 (URL이 없어도 키워드로 제거)
        r'Copyright\s*Ⓒ\s*(?:KBS|SBS|YTN)\.\s*All\s*rights\s*reserved\.\s*무단\s*전재,\s*재배포\s*(?:및\s*이용|\s*및\s*AI학습\s*이용)?\s*금지', # 저작권 문구
        r'\b(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}(?:/[^\s]*)?', # 일반적인 도메인 형태 제거 (http/https 없는 경우)
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', # 완벽한 URL 제거
    ]

    for line in lines:
        stripped_line = line.strip()
        
        # 각 패턴에 대해 검사
        is_junk = False
        for pattern in common_junk_patterns:
            if re.search(pattern, stripped_line, re.IGNORECASE):
                is_junk = True
                break
        
        if is_junk:
            continue
        
        cleaned_lines.append(stripped_line) # strip 된 상태로 추가
    
    text = ' '.join(cleaned_lines) # 다시 공백으로 조인

    # 2. 괄호 안 내용 제거 (괄호 안에 링크나 불필요한 정보가 있을 수 있으므로)
    text = re.sub(r'\([^()]*\)', ' ', text)
    text = re.sub(r'\{[^{}]*\}', ' ', text)
    text = re.sub(r'\[[^\[\]]*\]', ' ', text)

    # 3. 해시태그 제거 (줄 단위에서 제거되지 않은 경우를 대비)
    text = re.sub(r"#[\w가-힣]+", " ", text)

    # 4. 뉴스 종결 문구 제거 (단, 인물 발언이 아닌 문장 끝맺음)
    text = re.sub(r'(뉴스\s?[가-힣]{1,10}입니다[.]?)|(기자입니다[.]?)|(기잡니다[.]?)|(보도합니다[.]?)|(전합니다[.]?)|(전해드립니다[.]?)', ' ', text)
    text = re.sub(r'[가-힣]{2,10}\s*:\s*[가-힣\s]{2,100}', ' ', text) # "이름: 발언" 형식 제거 (YTN 유투권 (r2kwon@ytn.co.kr) 같은 형태)
    text = re.sub(r'[가-힣]{2,7}\s*:\s*[가-힣]{2,6}(?:[ /·ㆍ,][가-힣]{2,6})*', ' ', text) # 짧은 "이름: 이름" 형식 제거
    
    # 5. 숫자 및 단위 제거 (앞에서 처리했으나 한 번 더 보강)
    text = re.sub(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?[가-힣a-zA-Z%℃]+\b', ' ', text)
    text = re.sub(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b', ' ', text)
    text = re.sub(r'#\s*\d+', ' ', text) # '# 6000' 같은 문자 메시지 번호 제거

    # 6. 따옴표 및 불필요한 기호 제거
    text = re.sub(r'["“”‘’\'`]', ' ', text)
    text = re.sub(r'([가-힣ㅏ-ㅣ])\1{1,}', '', text) # 자음/모음 반복 제거
    text = re.sub(r'[ㅋㅎㅠㅜ]{2,}', '', text) # 감탄사 자음/모음 반복 제거
    text = re.sub(r'[^\w\s가-힣.!?,]', ' ', text) # 한글, 영어, 숫자, 공백, 일반적인 문장 부호(.!?,)만 남김
    
    # 7. 다중 공백을 단일 공백으로 변환하고 양 끝 공백 제거
    text = re.sub(r'\s+', ' ', text).strip()
    return text.strip()


def clean_text_for_vectorization(text):
    if not isinstance(text,str):
        return ""
    # 이 단계에서 'Copyright', '해시태그', '기사 원문보기' 패턴 등이 제거됩니다.
    cleaned_initial_text = clean_text_full(text)
    cleaned_initial_text = re.sub(r'([가-힣ㅏ-ㅣ])\1{1,}', '', cleaned_initial_text) # 자음/모음 반복 제거
    cleaned_initial_text = re.sub(r'[ㅋㅎㅠㅜ]{2,}', '', cleaned_initial_text) # 감탄사 자음/모음 반복 제거
    cleaned_initial_text = re.sub(r'[^\w\s가-힣.]', ' ', cleaned_initial_text) # 한글, 영어, 숫자, 공백, . 외 제거
    cleaned_initial_text = re.sub(r'\s+', ' ', cleaned_initial_text).strip() # 다중 공백 제거 및 양 끝 공백 제거
    # 명사(Noun), 동사(Verb), 형용사(Adjective), 부사(Adverb)만 선택
    words = []
    # norm=True: 원형 복원, stem=True: 어간 추출 (동사/형용사 등)
    for word, tag in okt.pos(cleaned_initial_text, norm=True, stem=True):
        if tag in ['Noun', 'Verb', 'Adjective', 'Adverb']:
            words.append(word)
    # 4. 사용자 정의 불용어 제거
    korean_stopwords = [
        '오늘', '이번', '지난', '되다', '하다', '있다', '이다', '것', '수', '그', '더', '좀', '잘',
        '가장', '다', '또', '많이', '그리고', '그러나', '하지만', '따라', '등', '등등', '통해',
        '까지', '부터', '대한', '으로', '에서', '에게', '에게서', '보다', '때문',
        '습니다', '합니다', '합니다만', '입니다만', '이라고', '이었습니다', '였습니다', # 종결어미 형태소
        '년', '월', '일', '시', '분', '초', '오전', '오후', '이번주', '지난주', '다음주', '이달', '지난달', '다음달', '올해', '지난해', '내년',
        '명', '원', '씨', '보시', '하시', '들어보시', '확인해보시', '시키', '알아보시', '해보시', # 불완전하게 남는 동사/형용사 어간 (추가)
        '어서', '으니', '으면', '어도', '으니', '으러', # 자주 남는 연결어미 등 (추가)
        '앱', '설치', '뉴스', '제보', '채널', '구독', '라이브', '모바일', '24', '홈페이지', '카카오톡', '페이스북', '이메일', '문자', '전화', '친구', '채팅', '메시지', '전송', '스프', '토론', '구독하기', '만나다', '보기', '검색', '전송', '누르다', '뜨겁다', '이슈', '함께', '토론', '기사', '모으다', '실시간', '문의', '연락처', '연결', '링크', '클릭', '자세하다' # 추가 불용어
    ]
    # 한 글자 단어는 일반적으로 의미가 약하므로 불용어 목록에 없어도 제거
    words = [word for word in words if word not in korean_stopwords and len(word) > 1]
    # 5. 공백으로 조인하여 벡터화에 적합한 형태로 반환
    return ' '.join(words)

# --- WebDriver 초기화 함수 ---
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

# --- 시간 파싱 도우미 함수 (1단계에서 사용) ---
def parse_duration_to_seconds(duration_str):
    parts = list(map(int, duration_str.split(':')))
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    elif len(parts) == 2:
        return parts[0] * 60 + parts[1]
    return 0

# 96시간 이내로 조건을 확장 -> 24시간으로 유지
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
        return days <= (max_hours // 24) # 'X일 전'이 MAX_VIDEO_AGE_HOURS를 넘지 않도록
    return False

# 영문 영상 필터링을 위한 함수 추가
def is_english_video(title, description):
    # 제목과 설명이 비어있으면 언어 감지에서 제외 (한국어로 간주하거나, 나중에 걸러질 것)
    if not title and not description:
        return False
    
    title_is_english = False
    description_is_english = False

    try:
        if title and len(title.strip()) > 5: # 너무 짧은 제목은 오탐 가능성 있으므로 길이 조건 추가
            if detect(title) == 'en':
                title_is_english = True
    except Exception:
        pass # 언어 감지 실패 시 (텍스트가 너무 짧거나 이상한 경우)

    try:
        if description and len(description.strip()) > 20: # 설명이 너무 짧으면 오탐 가능성 있으므로 길이 조건 추가
            if detect(description) == 'en':
                description_is_english = True
    except Exception:
        pass # 언어 감지 실패 시

    # 제목과 설명 모두 영어가 확실할 때만 True 반환 (두 조건 모두 충족)
    return title_is_english and description_is_english

# --- 1단계 - 유튜브 메인 페이지 동영상 목록 크롤링 및 빠른 필터링 ---
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

                # 24시간 이내 조건 확인 (1단계에서는 '일 전' 단위로 대략적인 필터링)
                # MAX_VIDEO_AGE_HOURS가 24이므로, 24 // 24 = 1
                if "일 전" in upload_time_str:
                    days_ago = int(re.search(r'(\d+)', upload_time_str).group(1))
                    if days_ago >= (MAX_VIDEO_AGE_HOURS // 24) :
                        print(f"'{upload_time_str}' 동영상 발견. 설정된 {MAX_VIDEO_AGE_HOURS}시간을 초과하는 영상이 많아 1단계 크롤링을 중단합니다.")
                        stop_scrolling = True
                        break
                
                # 1단계에서는 is_within_hours를 사용하지 않고, 대략적인 정보로 빠르게 필터링
                # 정확한 시간 필터링은 2단계에서 yt-dlp 데이터로 수행
                
                duration_div = video.select_one('badge-shape.badge-shape-wiz--thumbnail-default div.badge-shape-wiz__text')
                if not duration_div:
                    continue
                duration_str = duration_div.get_text(strip=True)
                video_length_seconds = parse_duration_to_seconds(duration_str)

                if not (MIN_VIDEO_LENGTH_SECONDS <= video_length_seconds <= MAX_VIDEO_LENGTH_SECONDS):
                    continue

                if video_url_suffix and upload_time_str:
                    full_url = f"https://www.youtube.com{video_url_suffix}" 
                    filtered_video_urls_1st_stage.append({
                        "url": full_url,
                        "upload_time_summary": upload_time_str,
                    })
                    seen_video_ids.add(video_id)
                    current_scroll_videos_count += 1

            except (NoSuchElementException, StaleElementReferenceException, AttributeError, IndexError, TypeError) as e:
                continue
            except Exception as e:
                # print(f"1단계 크롤링 중 예상치 못한 오류 발생: {e} (URL: {video.select_one('a#thumbnail')['href'] if video.select_one('a#thumbnail') else 'N/A'})")
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

# yt-dlp를 이용한 개별 영상 상세 정보 크롤링 함수 ---
# 이 함수는 Pool.map을 통해 병렬로 실행될 것이므로, 필요한 모든 데이터를 인자로 받도록 수정하고,
# 전처리 및 최종 필터링 로직을 이 함수 내부에 포함시킵니다.
def process_video_data(video_data_1st_stage):
    korean_stopwords = [ # Okt 객체가 직렬화되지 않으므로, 불용어 리스트는 함수 내부에 정의하거나 글로벌 변수로 재정의
        '오늘', '이번', '지난', '되다', '하다', '있다', '이다', '것', '수', '그', '더', '좀', '잘',
        '가장', '다', '또', '많이', '그리고', '그러나', '하지만', '따라', '등', '등등', '통해',
        '까지', '부터', '대한', '으로', '에서', '에게', '에게서', '보다', '때문',
        '습니다', '합니다', '합니다만', '입니다만', '이라고', '이었습니다', '였습니다', # 종결어미 형태소
        '년', '월', '일', '시', '분', '초', '오전', '오후', '이번주', '지난주', '다음주', '이달', '지난달', '다음달', '올해', '지난해', '내년',
        '명', '원', '씨', '보시', '하시', '들어보시', '확인해보시', '시키', '알아보시', '해보시', # 불완전하게 남는 동사/형용사 어간 (추가)
        '어서', '으니', '으면', '어도', '으니', '으러', # 자주 남는 연결어미 등 (추가)
        '앱', '설치', '뉴스', '제보', '채널', '구독', '라이브', '모바일', '24', '홈페이지', '카카오톡', '페이스북', '이메일', '문자', '전화', '친구', '채팅', '메시지', '전송', '스프', '토론', '구독하기', '만나다', '보기', '검색', '전송', '누르다', '뜨겁다', '이슈', '함께', '토론', '기사', '모으다', '실시간', '문의', '연락처', '연결', '링크', '클릭', '자세하다' # 추가 불용어
    ]
    # Okt 객체는 각 프로세스에서 생성
    _okt = Okt()

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
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            video_info = ydl.extract_info(video_url, download=False)

        title = video_info.get('title', '제목 없음')
        description = video_info.get('description', '설명 없음')
        webpage_url = video_info.get('webpage_url', video_url)
        thumbnail = video_info.get('thumbnail', None)
        if 'thumbnails' in video_info and video_info['thumbnails']:
            thumbnail = video_info['thumbnails'][-1].get('url', thumbnail)
        
        view_count = video_info.get('view_count') # 조회수 정보 추가

        upload_date_kst_formatted = None
        if 'timestamp' in video_info and video_info['timestamp'] is not None:
            dt_object_utc = datetime.fromtimestamp(video_info['timestamp'], tz=pytz.utc)
            kst_timezone = pytz.timezone('Asia/Seoul')
            dt_object_kst = dt_object_utc.astimezone(kst_timezone)
            upload_date_kst_formatted = dt_object_kst.strftime('%Y-%m-%d %H:%M:%S')

        # 24시간 필터링
        now_kst = datetime.now(KST) # 전역 변수 KST 사용
        if upload_date_kst_formatted:
            try:
                uploaded_dt_kst = KST.localize(datetime.strptime(upload_date_kst_formatted, '%Y-%m-%d %H:%M:%S'))
                time_difference = now_kst - uploaded_dt_kst
                if time_difference.total_seconds() >= MAX_VIDEO_AGE_HOURS * 3600:
                    print(f" -> '{upload_date_kst_formatted}' (KST) 영상은 {MAX_VIDEO_AGE_HOURS}시간을 초과하여 제외합니다.")
                    return None # 24시간 초과 영상은 None 반환하여 최종 목록에서 제외
            except ValueError:
                print(f" -> 경고: KST 업로드 시간 '{upload_date_kst_formatted}' 파싱 오류. 필터링하지 않고 포함합니다.")
        else:
            print(f" -> 경고: YT-DLP에서 KST 업로드 시간을 가져오지 못했습니다. 이 영상은 필터링하지 않고 포함합니다.")

        # 영문 영상 필터링
        if is_english_video(title, description):
            print(f" -> 영문 영상으로 판단되어 제외합니다: '{title}'")
            return None

        # 전처리 함수 적용
        cleaned_title_result = clean_title(title)
        cleaned_description_result = clean_text_for_vectorization_for_multiprocessing(description, _okt, korean_stopwords)

        return {
            "title": title,
            "cleaned_title": cleaned_title_result,
            "upload_date_kst": upload_date_kst_formatted,
            "description": description,
            "cleaned_description": cleaned_description_result,
            "video_link": webpage_url,
            "thumbnail_link": thumbnail,
            "view_count": view_count # 조회수 추가
        }

    except yt_dlp.utils.DownloadError as e:
        print(f"yt-dlp 오류 발생 (URL: {video_url}): {e}")
        return None
    except Exception as e:
        print(f"yt-dlp 처리 중 예상치 못한 오류 발생 (URL: {video_url}): {e}")
        return None

# clean_text_for_vectorization 함수를 multiprocessing에서 사용할 수 있도록 수정
# Okt 객체와 불용어 리스트를 인자로 받도록 변경
def clean_text_for_vectorization_for_multiprocessing(text, _okt_instance, _korean_stopwords):
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

if __name__ == "__main__":
    driver = initialize_webdriver()

    if driver:
        crawl_main_page_and_filter_videos(driver)
        driver.quit() # 1단계 완료 후 WebDriver 종료
        print("WebDriver 종료.")
    else:
        print("WebDriver가 초기화되지 않아 1단계 크롤링을 실행할 수 없습니다. 프로그램을 종료합니다.")
        exit()

    print("\n2단계 크롤링 및 전처리 시작 (병렬 처리)...")
    total_start_time = time.time()

    if filtered_video_urls_1st_stage:
        # 코어 수 확인 및 워커 수 설정
        num_cores = cpu_count()
        # 오버헤드를 고려하여 코어 수 - 1 또는 코어 수와 동일하게 설정할 수 있습니다.
        # 여기서는 최대 효율을 위해 코어 수와 동일하게 설정합니다.
        num_workers = num_cores
        print(f"시스템 코어 수: {num_cores}, 설정된 워커 수: {num_workers}")

        temp_final_data_before_id = []

        # Pool을 사용하여 병렬 처리
        # process_video_data 함수에 filtered_video_urls_1st_stage의 각 항목을 전달
        # map 함수는 모든 결과를 기다린 후 리스트로 반환합니다.
        with Pool(processes=num_workers) as pool:
            # starmap은 여러 인자를 넘길 때 사용하지만, 여기서는 단일 인자이므로 map을 사용합니다.
            # map의 결과는 순서가 유지됩니다.
            results = pool.map(process_video_data, filtered_video_urls_1st_stage)

        # 병렬 처리된 결과들을 취합하고 필터링
        for detailed_info in results:
            if detailed_info: # None이 아닌 유효한 데이터만 추가
                temp_final_data_before_id.append(detailed_info)

        print(f"\n총 {len(temp_final_data_before_id)}개의 유효한 영상 상세 정보 및 전처리 완료.")

        # 최종 필터링된 데이터를 최신순으로 정렬하고 순차적인 ID 부여
        if temp_final_data_before_id:
            temp_final_data_before_id.sort(
                key=lambda x: datetime.strptime(x['upload_date_kst'], '%Y-%m-%d %H:%M:%S') if x.get('upload_date_kst') else datetime.min,
                reverse=True
            )

            for i, item in enumerate(temp_final_data_before_id):
                item['id'] = i + 1
                final_video_data.append(item)

        output_filename = "예시_크롤링파일.json" # 파일 이름은 그대로 유지
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
                        "thumbnail_link": item.get("thumbnail_link"),
                        "view_count": item.get("view_count") # 조회수 필드 추가
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