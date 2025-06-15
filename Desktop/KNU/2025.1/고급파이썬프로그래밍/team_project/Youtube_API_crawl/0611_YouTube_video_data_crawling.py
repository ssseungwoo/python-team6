import time
import json
import re
from datetime import datetime, timedelta

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
import pytz
from konlpy.tag import Okt

okt = Okt()

YOUTUBE_MAIN_URL = "https://www.youtube.com/channel/UCcQTRi69dsVYHN3exePtZ1A/videos/videos"
SCROLL_PAUSE_TIME = 2 # 스크롤 대기 시간 2초설정
MAX_SCROLLS = 500 # 최대 500번 스크롤, 무한대로 스크롤 하는 거 방지 위함

MIN_VIDEO_LENGTH_SECONDS = 60   # 크롤링할 영상 길이 조건 
MAX_VIDEO_LENGTH_SECONDS = 300  # 1~5분 영상
KST = pytz.timezone('Asia/Seoul') # KST 시간대 설정

filtered_video_urls_1st_stage = []  # 1단계 크롤링 결과를 저장할 리스트
final_video_data = []   # 최종 크롤링 결과를 저장할 리스트

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
    # 1. '▣ KBS 기사 원문보기 : ' 패턴 이후의 모든 내용 삭제 (가장 먼저 적용)
    text = re.sub(r"^(▣|◇|■|▲|▼|◆|○|●|△|▷|▶|※|◎)\s*(KBS|SBS|YTN)\s*기사 원문보기\s*:\s*.*", "", text, flags=re.DOTALL)
    # 2. **강화된 '무단 배포 이용 금지' 및 Copyright 문구 제거**
    text = re.sub(r"무단\s*배포\s*이용\s*금지", "", text, flags=re.IGNORECASE)
    text = re.sub(r"무단\s*전재,\s*재배포\s*및\s*이용(?:\(AI\s*학습\s*포함\))?\s*금지", "", text, flags=re.IGNORECASE)
    # 기존의 Copyright 패턴 제거 (위에서 처리되지 않은 다른 Copyright 형태를 위함)
    text = re.sub(r"Copyright\s*©.*(?:All rights reserved\..*)?", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"Copyright\s*©.*", "", text, flags=re.DOTALL | re.IGNORECASE) 
    # 3. 해시태그 제거
    text = re.sub(r"#[\w가-힣]+", "", text) 
    # 4. 문장 시작 기호 제거
    text = re.sub(r'^(▣|◇|■|▲|▼|◆|○|●|△|▷|▶|※|◎)\s*', ' ', text)
    # 5. 괄호 안 내용 제거
    text = re.sub(r'\([^()]*\)', ' ', text)
    text = re.sub(r'\{[^{}]*\}', ' ', text)
    text = re.sub(r'\[[^\[\]]*\]', ' ', text)
    # 6. 특정 기호로 시작하는 줄 전체 제거 (주로 뉴스 기사 정보)
    lines = text.splitlines()
    lines = [line for line in lines if not line.strip().startswith(('▣', '◇', '■', '▲', '▼', '◆', '○', '●', '△', '▷', '▶', '※', '◎'))]
    text = ' '.join(lines)
    # 7. '~습니다', '~합니다'와 같은 존대 표현의 일부 제거 (뉴스 스크립트 특화)
    text = re.sub(r'([가-힣]{2,4})(?:[이|임|섭|립|음|합|습|랍]?)\b니다', ' ', text)
    text = re.sub(r'([가-힣]{2,4})(?:[이|임|섭|립|음|합|습|랍]?)\b습니다', ' ', text)
    # 8. 기자/앵커 정보 패턴 제거
    job_titles = '기자|앵커|특파원|기상캐스터|진행|촬영기자|그래픽|리포터|논설위원|논평|해설위원|취재|현장기자|뉴스팀|보도국|영상편집'
    text = re.sub(rf'([가-힣]{{2,4}}\s*(?:{job_titles})(?:\s*[:/]\s*[가-힣]{{2,4}}\s*(?:{job_titles})?)*)', ' ', text)
    text = re.sub(rf'((?:{job_titles})\s*[:/]\s*[가-힣]{{2,4}}(?:\s*[:/]\s*(?:{job_titles})?\s*[가-힣]{{2,4}})*)', ' ', text)
    endings = '입니다|입니다\\.|입니다\\?|이다|입니다만|입니다만\\.|이라고|이라는|라고|는|은|가|이었습니다\\.|이었습니다|였습니다\\.|였습니다'
    text = re.sub(rf'((?:{job_titles})\s*[:/]\s*[가-힣]{{2,4}}(?:\s*[:/]\s*(?:{job_titles})?\s*[가-힣]{{2,4}})*)', ' ', text)
    # 9. 특정 구문 제거 (뉴스 말미 표현)
    text = re.sub(r'\'[가-힣\s]+\'\s*[가-힣]{2,10}(?:[ ]*였습니다\\.|였습니다|이었습니다\\.|이었습니다)\s*', ' ', text)
    text = re.sub(r'[가-힣]{2,4}의\s+[가-힣\s]+(?:[ ]*였습니다\\.|였습니다|이었습니다\\.|이었습니다)\s*', ' ', text)
    # 10. 기자/앵커 직책만 제거
    text = re.sub(rf'([가-힣]{{2,4}}\s*(?:{job_titles})\s*(?:{endings})\s*)', ' ', text)
    text = re.sub(rf'([가-힣]{{2,4}}\s*(?:{job_titles})\s*)', ' ', text)
    text = re.sub(r'\b(기자|앵커|특파원|기상캐스터|진행|촬영기자|그래픽|리포터|논설위원|논평|해설위원|취재|현장기자|뉴스팀|보도국)\b', ' ', text)
    # 11. 뉴스 종결 문구 제거
    text = re.sub(r'(뉴스\s?[가-힣]{1,10}입니다[.]?)|(기자입니다[.]?)|(기잡니다[.]?)|(보도합니다[.]?)|(전합니다[.]?)|(전해드립니다[.]?)', ' ', text)
    text = re.sub(r'[가-힣]{2,10}\s*:\s*[가-힣\s]{2,100}', ' ', text) # "이름: 발언" 형식 제거
    text = re.sub(r'[가-힣]{2,7}\s*:\s*[가-힣]{2,6}(?:[ /·ㆍ,][가-힣]{2,6})*', ' ', text) # 짧은 "이름: 이름" 형식 제거
    # 12. 따옴표 제거
    text = re.sub(r'["“”‘’\'`]', ' ', text)
    # 13. 특정 방송사 뉴스 문구 제거
    text = re.sub(r'(KBS|SBS|YTN)\s뉴스\s[가-힣]{1,4}((입|이|합|습|랍)?입니다)', ' ', text)
    text = re.sub(r'\b(KBS|SBS|YTN)\b', ' ', text)
    # 14. 숫자 및 단위 제거 (예: 123원, 50%)
    text = re.sub(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?[가-힣a-zA-Z%℃]+\b', ' ', text)
    text = re.sub(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b', ' ', text) 
    # 15. 다중 공백을 단일 공백으로 변환하고 양 끝 공백 제거
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
    ]
    # 한 글자 단어는 일반적으로 의미가 약하므로 불용어 목록에 없어도 제거
    words = [word for word in words if word not in korean_stopwords and len(word) > 1]
    # 5. 공백으로 조인하여 벡터화에 적합한 형태로 반환
    return ' '.join(words)

# --- WebDriver 초기화 함수 ---
def initialize_webdriver():
    chrome_options = Options()
    chrome_options.add_argument("--headless") # 크롬 브라우저 백그라운드에서 실행
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--incognito")  # 시크릿 모드
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
    if len(parts) == 3:  # 시:분:초
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    elif len(parts) == 2:  # 분:초
        return parts[0] * 60 + parts[1]
    return 0

def is_within_24_hours(upload_time_str):
    upload_time_str = upload_time_str.replace(" ", "")  # 공백 제거

    if "분전" in upload_time_str:
        minutes = int(re.search(r'(\d+)', upload_time_str).group(1))
        return minutes <= 60 * 24  # 24시간 = 1440분
    elif "시간전" in upload_time_str:
        hours = int(re.search(r'(\d+)', upload_time_str).group(1))
        return hours <= 24
    elif "일전" in upload_time_str:
        days = int(re.search(r'(\d+)', upload_time_str).group(1))
        # 1일 전이라도 정확히 24시간을 넘지 않았을 수 있으므로 포함합니다 (2단계에서 정밀 필터링).
        return days < 1 or (days == 1 and "시간전" not in upload_time_str)  # '1일 전' 이면 일단 포함
    return False  # 기타 형식 (예: '주 전', '개월 전')은 24시간 초과로 간주


# --- 1단계 - 유튜브 메인 페이지 동영상 목록 크롤링 및 빠른 필터링 ---
def crawl_main_page_and_filter_videos(driver):
    print("1단계 크롤링 시작: 메인 페이지 동영상 목록 필터링")
    try:
        driver.get(YOUTUBE_MAIN_URL)
        print(f"{YOUTUBE_MAIN_URL} 접속 완료.")
        # 페이지 로딩 대기 (동영상 컨텐츠가 나타날 때까지)
        WebDriverWait(driver, 30).until(  # 타임아웃을 30초로 늘려 안정성 확보
            EC.presence_of_element_located((By.ID, "contents"))
        )
        print("페이지 컨텐츠 로딩 완료.")
    except TimeoutException:
        print("페이지 컨텐츠 로딩 시간 초과. 네트워크 상태를 확인하거나 타임아웃을 늘리세요.")
        return
    except WebDriverException as e:
        print(f"유튜브 메인 페이지 접속 중 오류 발생: {e}")
        return

    seen_video_ids = set()  # 중복 방지를 위해 이미 처리한 영상 ID 저장
    scroll_count = 0
    last_height = driver.execute_script("return document.documentElement.scrollHeight")
    stop_scrolling = False
    no_change_count = 0  # 높이 변화가 없는 횟수를 세는 카운터

    while not stop_scrolling and scroll_count < MAX_SCROLLS:
        print(f"\n스크롤 {scroll_count + 1}회 시작...")
        driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME + 1)  # 스크롤 후 새로운 컨텐츠 로딩 대기 (기존 2초 + 1초 = 3초)

        new_height = driver.execute_script("return document.documentElement.scrollHeight")

        if new_height == last_height:
            no_change_count += 1
            print(f"높이 변화 없음 (연속 {no_change_count}회).")
            if no_change_count >= 3:  # 3번 연속 변화가 없으면 중단
                print("3회 연속 스크롤 높이 변화 없음. 더 이상 로드할 컨텐츠가 없거나 로딩이 매우 느립니다. 중단합니다.")
                break
        else:
            no_change_count = 0  # 높이 변화가 있으면 카운터 초기화

        last_height = new_height

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        video_containers = soup.select('ytd-rich-item-renderer, ytd-video-renderer') # 동영상 요소 찾음
        if not video_containers:
            print("경고: 동영상 컨테이너 요소를 찾을 수 없습니다. 셀렉터를 확인하세요.")
            if no_change_count >= 3:
                break
            continue

        current_scroll_videos_count = 0
        for video in video_containers:
            try:
                link_tag = video.select_one('a#thumbnail') # 영상 링크 및 ID 추출
                if not link_tag or 'href' not in link_tag.attrs:
                    continue
                video_url_suffix = link_tag['href']
                if not video_url_suffix.startswith('/watch?v='): # 유효한 동영상 링크인지 확인
                    continue
                video_id = video_url_suffix.split('v=')[1].split('&')[0] # 고유 ID 추출
                if video_id in seen_video_ids:
                    continue  # 이미 처리한 영상은 건너뛰기

                # 업로드 시간 추출
                upload_time_element = video.select_one('div#metadata-line span.inline-metadata-item:last-of-type') 
                upload_time_str = upload_time_element.get_text(strip=True) if upload_time_element else ""
                if not upload_time_str or "전" not in upload_time_str:
                    continue  # 업로드 시간이 없거나 '전'이라는 단어가 포함되지 않은 경우 스킵, 라이브 영상도 제외
                
                
                if "일 전" in upload_time_str:  # 크롤링 중단 조건 : 일 전 발견 시 크롤링 중지, 빠르게 조건에 비해 오래된 영상 스캔을 멈추는 기준입니다.
                    days_ago = int(re.search(r'(\d+)', upload_time_str).group(1))
                    if days_ago >= 1:   # '1일 전'이거나 그 이상이면 중단 (정확한 24시간 필터링은 2단계에서)
                        print(f"'{upload_time_str}' 동영상 발견. 1단계 크롤링을 중단합니다.")
                        stop_scrolling = True
                        break 
                # 1단계 업로드 시간 필터링 (대략적인 기준)
                if not is_within_24_hours(upload_time_str):
                    continue
                # 영상 길이 추출 및 필터링
                duration_div = video.select_one('badge-shape.badge-shape-wiz--thumbnail-default div.badge-shape-wiz__text')
                if not duration_div:
                    continue
                duration_str = duration_div.get_text(strip=True)
                video_length_seconds = parse_duration_to_seconds(duration_str)

                if not (MIN_VIDEO_LENGTH_SECONDS <= video_length_seconds <= MAX_VIDEO_LENGTH_SECONDS):
                    continue  # 길이 조건 불만족
                
                # 모든 조건 만족 시 저장
                if video_url_suffix and upload_time_str:
                    full_url = f"{YOUTUBE_MAIN_URL}{video_url_suffix}" # 완전한 URL 생성
                    filtered_video_urls_1st_stage.append({
                        "url": full_url,
                        "upload_time_summary": upload_time_str,  # 1단계 필터링용 요약 시간
                    })
                    seen_video_ids.add(video_id)  # 최종적으로 추가된 영상만 seen_video_ids에 넣기
                    current_scroll_videos_count += 1

            except (NoSuchElementException, StaleElementReferenceException, AttributeError, IndexError, TypeError) as e:
                continue
            except Exception as e:
                # print(f"처리 중 예상치 못한 오류 발생: {e}") # 디버깅 시에만 사용
                continue

        print(f"현재 스크롤에서 {current_scroll_videos_count}개의 유효한 동영상 후보 발견.")
        if stop_scrolling:
            break

        scroll_count += 1
        time.sleep(1)  # 짧은 대기 (과도한 요청 방지)

    print(f"\n1단계 크롤링 완료. 총 {len(filtered_video_urls_1st_stage)}개의 동영상 후보가 수집되었습니다.")
    # 최종적으로 중복 제거
    unique_videos_dict = {video['url']: video for video in filtered_video_urls_1st_stage}
    filtered_video_urls_1st_stage[:] = list(unique_videos_dict.values())
    print(f"최종 1단계 필터링 후 {len(filtered_video_urls_1st_stage)}개의 유니크한 동영상 후보.")

# yt-dlp를 이용한 개별 영상 상세 정보 크롤링 함수 ---
def get_youtube_video_details_yt_dlp(video_url):
    ydl_opts = {
        'noplaylist': True,
        'quiet': True,
        'no_warnings': True,
        'forcethumbnail': True,  # 썸네일 정보를 강제로 가져오도록 시도
        'skip_download': True,   # 실제 영상 다운로드 건너뛰기
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            video_info = ydl.extract_info(video_url, download=False)

            title = video_info.get('title', '제목 없음')
            description = video_info.get('description', '설명 없음')
            webpage_url = video_info.get('webpage_url', video_url)  # 추출한 실제 영상 링크
            
            thumbnail = video_info.get('thumbnail', None)
            if 'thumbnails' in video_info and video_info['thumbnails']:
                thumbnail = video_info['thumbnails'][-1].get('url', thumbnail)

            # KST 형식 날짜/시간 포맷팅
            upload_date_kst_formatted = None
            if 'timestamp' in video_info and video_info['timestamp'] is not None:
                dt_object_utc = datetime.fromtimestamp(video_info['timestamp'], tz=pytz.utc)
                kst_timezone = pytz.timezone('Asia/Seoul')
                dt_object_kst = dt_object_utc.astimezone(kst_timezone)
                upload_date_kst_formatted = dt_object_kst.strftime('%Y-%m-%d %H:%M:%S')
            
            return {
                "title": title,
                "upload_date_kst": upload_date_kst_formatted,  # KST 업로드 시간
                "description": description,
                "video_link": webpage_url,
                "thumbnail_link": thumbnail
            }

    except yt_dlp.utils.DownloadError as e:
        print(f"yt-dlp 오류 발생 (URL: {video_url}): {e}")
        return None
    except Exception as e:
        print(f"yt-dlp 처리 중 예상치 못한 오류 발생 (URL: {video_url}): {e}")
        return None

# 메인 실행
if __name__ == "__main__":
    # WebDriver 초기화
    driver = initialize_webdriver()

    if driver:
        # 1단계 크롤링 실행: 유튜브 메인 페이지 동영상 목록 필터링
        crawl_main_page_and_filter_videos(driver)
    else:
        print("WebDriver가 초기화되지 않아 1단계 크롤링을 실행할 수 없습니다. 프로그램을 종료합니다.")
        exit() # WebDriver 초기화 실패 시 프로그램 종료

    print("\n2단계 크롤링 함수 정의 완료.")
    print("\n최종 데이터 크롤링 시작...")
    total_start_time = time.time()

    if filtered_video_urls_1st_stage:
        # 현재 시간 설정 (yt-dlp 결과와 비교하기 위함)
        kst_timezone = pytz.timezone('Asia/Seoul')
        now_kst = datetime.now(kst_timezone)
        
        # 2단계 크롤링 시 필요한 데이터를 담을 임시 리스트
        # 정렬 후 ID를 부여하기 위해 일단 모든 유효 데이터를 여기에 모음
        temp_final_data_before_id = [] 

        for i, video_data_1st_stage in enumerate(filtered_video_urls_1st_stage):
            video_url = video_data_1st_stage['url']
            print(f"[{i+1}/{len(filtered_video_urls_1st_stage)}] '{video_data_1st_stage.get('upload_time_summary', 'N/A')}' 동영상 상세 정보 크롤링 중: {video_url}")
            
            # yt-dlp 상세 정보 크롤링 함수 호출
            detailed_info = get_youtube_video_details_yt_dlp(video_url)
            
            if detailed_info:
                # 전처리 함수 적용
                original_title = detailed_info.get('title', '')
                original_description = detailed_info.get('description', '')

                # title에 clean_title 적용
                cleaned_title_result = clean_title(original_title)
                # description에 clean_text_for_vectorization (Okt 형태소 분석 포함) 적용
                cleaned_description_result = clean_text_for_vectorization(original_description)
                
                # 새로운 필드 추가
                detailed_info['cleaned_title'] = cleaned_title_result
                detailed_info['cleaned_description'] = cleaned_description_result

                upload_date_str_kst = detailed_info.get('upload_date_kst', None)
                
                # KST 업로드 시간이 존재하면 정확하게 24시간 필터링
                if upload_date_str_kst:
                    try:
                        uploaded_dt_kst = kst_timezone.localize(datetime.strptime(upload_date_str_kst, '%Y-%m-%d %H:%M:%S'))
                        
                        time_difference = now_kst - uploaded_dt_kst
                        
                        # 24시간(86400초) 이상이면 최종 데이터에 추가하지 않음
                        if time_difference.total_seconds() >= 24 * 3600:
                            print(f" -> '{upload_date_str_kst}' (KST) 영상은 24시간을 초과하여 제외합니다.")
                            time.sleep(0.5)  # 요청 간 짧은 대기 병목 대비
                            continue  # 이 영상은 최종 목록에서 제외하고 다음 영상으로 넘어감
                        
                    except ValueError:
                        print(f" -> 경고: KST 업로드 시간 '{upload_date_str_kst}' 파싱 오류. 필터링하지 않고 포함합니다.")
                else:
                    print(f" -> 경고: YT-DLP에서 KST 업로드 시간을 가져오지 못했습니다. 이 영상은 필터링하지 않고 포함합니다.")

                # 24시간 이내인 경우에만 임시 리스트에 추가
                temp_final_data_before_id.append(detailed_info)
                print(f" -> 제목: {detailed_info.get('title', 'N/A')}, 업로드: {detailed_info.get('upload_date_kst', 'N/A')}")
            else:
                print(f" -> 상세 정보 크롤링 실패 또는 정보 부족: {video_url}")
            
            time.sleep(0.5)  # 개별 영상 요청 간 짧은 지연

        # 최종 필터링된 데이터를 최신순으로 정렬하고 순차적인 ID 부여
        if temp_final_data_before_id:
            # 'upload_date_kst' 문자열을 datetime 객체로 변환하여 정확하게 최신순으로 정렬
            temp_final_data_before_id.sort(
                key=lambda x: datetime.strptime(x['upload_date_kst'], '%Y-%m-%d %H:%M:%S') if x.get('upload_date_kst') else datetime.min,
                reverse=True  # 내림차순 (최신 영상이 먼저 오도록)
            )
            
            for i, item in enumerate(temp_final_data_before_id): # 정렬된 순서대로 순차적인 ID 부여, 개수 파악 용도
                item['id'] = i + 1  # 1부터 시작하는 순번 부여
                final_video_data.append(item)

        print(f"\n총 {len(final_video_data)}개의 유효한 영상 상세 정보 크롤링 완료.")

        output_filename = "크롤링_샘플_데이터.json" # JSON 파일로 저장
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
    print(f"\n**전체 크롤링 작업 소요 시간**: {total_elapsed_time:.4f} 초") # 크롤링 소요 시간 파악

    if driver:
        driver.quit()
        print("WebDriver 종료.")