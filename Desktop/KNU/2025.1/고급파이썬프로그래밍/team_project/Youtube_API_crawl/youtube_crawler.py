import os
import re
from googleapiclient.discovery import build
from datetime import datetime, timedelta, timezone
import json
from urllib.parse import urlparse, parse_qs
import time
from konlpy.tag import Okt

# --- 설정값 (이 부분을 채워주세요!) ---
API_KEY = "AIzaSyBDQ2PKAsSzl13z_6I9IXPaKtO15vu48fY" # 발급받은 API 키를 여기에 입력하세요.

# Okt 객체는 한 번만 생성하여 재사용하는 것이 효율적입니다.
okt = Okt()

# --- ISO 8601 Duration 파싱 함수 ---
def parse_duration_iso8601(duration_str):
    """
    ISO 8601 형식의 duration 문자열 (예: PT1H30M10S)을 초 단위로 변환합니다.
    """
    total_seconds = 0
    duration_str = duration_str.replace('PT', '')

    hours_match = re.search(r'(\d+)H', duration_str)
    minutes_match = re.search(r'(\d+)M', duration_str)
    seconds_match = re.search(r'(\d+)S', duration_str)

    if hours_match:
        total_seconds += int(hours_match.group(1)) * 3600
    if minutes_match:
        total_seconds += int(minutes_match.group(1)) * 60
    if seconds_match:
        total_seconds += int(seconds_match.group(1))

    return total_seconds

# --- 텍스트 정제 함수들 ---
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
    # text = re.sub(r'\b(억|만|천|조|원|달러|개|명|세|도|℃|%|점|위|등|년|월|일|주|시간|분|초|배|가구|건|호|층|회|번|점|건|km|m|kg|g|명분|차례|시간대|도달)\b', ' ', text)
    
    text = re.sub(r'\s+', ' ', text).strip()
    return text.strip()

def clean_text_for_vectorization(text):
    if not isinstance(text,str):
        return ""
    
    # 기존 clean_text_full의 정규식 처리 적용
    cleaned_initial_text = clean_text_full(text)

    # 특수문자, 반복 자음/모음, 감탄사 등 추가 제거
    # clean_text_full에서 이미 유사한 처리가 있을 수 있으나, 더 명확하게 적용
    cleaned_initial_text = re.sub(r'([가-힣ㅏ-ㅣ])\1{1,}', '', cleaned_initial_text)
    cleaned_initial_text = re.sub(r'[ㅋㅎㅠㅜ]{2,}', '', cleaned_initial_text)
    cleaned_initial_text = re.sub(r'[^\w\s가-힣.]', ' ', cleaned_initial_text) # 한글, 영어, 숫자, 공백, . 외 제거
    cleaned_initial_text = re.sub(r'\s+', ' ', cleaned_initial_text).strip() # 다중 공백 제거 및 양 끝 공백 제거

    # 형태소 분석 및 품사 필터링
    # 명사(Noun), 동사(Verb), 형용사(Adjective), 부사(Adverb)만 선택
    words = []
    # norm=True: 원형 복원, stem=True: 어간 추출 (동사/형용사 등)
    for word, tag in okt.pos(cleaned_initial_text, norm=True, stem=True):
        if tag in ['Noun', 'Verb', 'Adjective', 'Adverb']:
            words.append(word)

    # 사용자 정의 불용어 제거 (옵션)
    korean_stopwords = [
        '오늘', '이번', '지난', '되다', '하다', '있다', '이다', '것', '수', '그', '더', '좀', '잘', 
        '가장', '다', '또', '많이', '그리고', '그러나', '하지만', '따라', '등', '등등', '통해', 
        '까지', '부터', '대한', '으로', '에서', '에게', '에게서', '보다', '때문',
        '습니다', '합니다', '합니다만', '입니다만', '이라고', '이었습니다', '였습니다', # 종결어미 형태소
        '년', '월', '일', '시', '분', '초', '오전', '오후', '이번주', '지난주', '다음주', '이달', '지난달', '다음달', '올해', '지난해', '내년'
    ]
    # 한 글자 단어는 일반적으로 의미가 약하므로 제거 (예: '나', '내', '줄' 등)
    words = [word for word in words if word not in korean_stopwords and len(word) > 1]

    # 공백으로 조인하여 벡터화에 적합한 형태로 반환
    return ' '.join(words)
    

# --- 채널 ID 추출 함수 ---
def get_channel_id_from_youtube_url(api_key, youtube_url):
    """
    YouTube URL에서 채널 ID를 추출합니다.
    필요에 따라 YouTube Data API를 호출할 수 있습니다.
    """
    youtube = build('youtube', 'v3', developerKey=api_key)
    parsed_url = urlparse(youtube_url)

    # 채널 ID (UC로 시작하는 24자) 직접 추출 시도
    # /channel/UC..., /user/username, /c/customurl, /@handle 등에서 ID 추출
    # /videos, /playlists 등 탭 경로가 붙더라도 채널 ID는 포함될 수 있음
    match_channel_id = re.search(r'(?:/channel/|/user/|/c/|/@)(UC[a-zA-Z0-9_-]{22})', youtube_url)
    if match_channel_id:
        print(f"URL에서 직접 채널 ID를 추출했습니다: {match_channel_id.group(1)}")
        return match_channel_id.group(1)

    path_segments = parsed_url.path.split('/')
    if len(path_segments) >= 2:
        # /user/username 형태 처리
        if path_segments[1] == 'user' and len(path_segments) > 2:
            username = path_segments[2]
            print(f"URL에서 사용자명 '{username}'을 추출했습니다. API로 채널 ID 검색...")
            try:
                request = youtube.channels().list(
                    part='id',
                    forUsername=username
                )
                response = request.execute()
                if response['items']:
                    return response['items'][0]['id']
            except Exception as e:
                print(f"사용자명 '{username}'으로 채널 ID를 검색하는 중 오류 발생: {e}")
        # /@handle 형태 처리 (YouTube Shorts나 새로운 채널 핸들 URL)
        elif path_segments[1].startswith('@') and len(path_segments) > 1:
            handle = path_segments[1][1:] # @ 제거
            print(f"URL에서 핸들 '{handle}'을 추출했습니다. API로 채널 ID 검색...")
            try:
                request = youtube.channels().list(
                    part='id',
                    forHandle=handle
                )
                response = request.execute()
                if response['items']:
                    return response['items'][0]['id']
            except Exception as e:
                print(f"핸들 '{handle}'으로 채널 ID를 검색하는 중 오류 발생: {e}")

    # 특정 영상 URL (예: watch?v=VIDEO_ID)에서 채널 ID 추출
    if 'watch' in parsed_url.path and 'v' in parse_qs(parsed_url.query):
        video_id = parse_qs(parsed_url.query)['v'][0]
        print(f"URL에서 영상 ID '{video_id}'를 추출했습니다. API로 채널 ID 검색...")
        try:
            request = youtube.videos().list(
                part='snippet',
                id=video_id
            )
            response = request.execute()
            if response['items']:
                return response['items'][0]['snippet']['channelId']
        except Exception as e:
            print(f"영상 ID '{video_id}'로 채널 ID를 검색하는 중 오류 발생: {e}")
            
    print("오류: 제공된 URL에서 채널 ID를 추출할 수 없습니다. 올바른 YouTube 채널 또는 영상 URL을 입력해주세요.")
    return None

# --- 영상 정보 크롤링 함수 ---
def get_recent_videos_with_duration_filter(api_key, channel_id, hours=24, min_duration_sec=60, max_duration_sec=300, max_results_per_page=50):
    """
    지정된 채널에서 최근 N시간 이내에 업로드된, 특정 길이 범위 내의 동영상 정보를 가져옵니다.
    """
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    # 현재 UTC 시간을 기준으로 필터링 시간대 설정
    now_utc = datetime.now(timezone.utc)
    threshold_time = now_utc - timedelta(hours=hours)

    filtered_videos = []
    next_page_token = None
    video_counter = 0 # ID 부여를 위한 카운터

    print(f"[{now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')} 기준] 최근 {hours}시간 ({threshold_time.strftime('%Y-%m-%d %H:%M:%S %Z')} 이후) 영상 탐색 시작...")
    print(f"영상 길이 조건: {min_duration_sec}초 ({min_duration_sec // 60}분) ~ {max_duration_sec}초 ({max_duration_sec // 60}분)")

    try:
        # 1. 채널의 '업로드' 플레이리스트 ID를 가져옵니다.
        channel_request = youtube.channels().list(
            part='contentDetails',
            id=channel_id
        )
        channel_response = channel_request.execute()

        if not channel_response['items']:
            print(f"오류: 채널 ID '{channel_id}'의 업로드 플레이리스트를 찾을 수 없습니다.")
            return []

        uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        print(f"채널의 '업로드' 플레이리스트 ID: {uploads_playlist_id}")

        # 2. '업로드' 플레이리스트의 동영상 목록을 페이지별로 가져옵니다. (최신순)
        while True:
            playlist_items_request = youtube.playlistItems().list(
                part='snippet,contentDetails', # snippet: 제목, 설명, 썸네일, publishedAt / contentDetails: videoId
                playlistId=uploads_playlist_id,
                maxResults=max_results_per_page,
                pageToken=next_page_token
            )
            playlist_items_response = playlist_items_request.execute()

            video_ids_on_page = []
            videos_data_from_playlist = {} # publishedAt을 저장하기 위해 사용

            for item in playlist_items_response['items']:
                published_at_str = item['snippet']['publishedAt']
                # UTC 시간으로 변환 (publishedAt은 ISO 8601 형식이며 시간대가 포함되어 있음)
                published_at = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%S%z').astimezone(timezone.utc)

                # 24시간 필터링 (먼저 적용하여 불필요한 duration API 호출 방지)
                # '업로드' 플레이리스트는 최신순이므로, 임계치보다 오래된 영상을 만나면 더 이상 탐색할 필요가 없습니다.
                if published_at < threshold_time:
                    print(f"[{published_at.strftime('%Y-%m-%d %H:%M:%S %Z')}] 임계치({threshold_time.strftime('%Y-%m-%d %H:%M:%S %Z')})보다 오래된 영상 발견. 추가 탐색 중단.")
                    return filtered_videos # 현재까지 수집된 영상만 반환하고 종료 (이전 페이지 포함)

                video_id = item['contentDetails']['videoId']
                video_ids_on_page.append(video_id)
                
                # playlistItems에서 바로 얻을 수 있는 정보만 먼저 저장
                videos_data_from_playlist[video_id] = {
                    'title': item['snippet']['title'],
                    'description': item['snippet']['description'],
                    'link': f"https://www.youtube.com/watch?v={video_id}", # 실제 YouTube 영상 링크 수정
                    'publishedAt': published_at_str,
                    'thumbnail_link': item['snippet']['thumbnails']['high']['url']
                }
            
            # 3. 현재 페이지의 모든 동영상 ID에 대해 duration 정보를 가져오기 위해 videos.list 호출
            if video_ids_on_page: # 현재 페이지에 영상 ID가 있다면
                videos_details_request = youtube.videos().list(
                    part='contentDetails', # duration을 포함하는 contentDetails 부분만 요청
                    id=','.join(video_ids_on_page) # 콤마로 연결된 ID 리스트
                )
                videos_details_response = videos_details_request.execute()

                for video_detail_item in videos_details_response['items']:
                    video_id = video_detail_item['id']
                    duration_iso8601 = video_detail_item['contentDetails']['duration']
                    duration_seconds = parse_duration_iso8601(duration_iso8601)

                    # 1분 ~ 5분 길이 필터링
                    if min_duration_sec <= duration_seconds <= max_duration_sec:
                        # 이미 playlistItems에서 가져온 데이터와 duration을 결합
                        if video_id in videos_data_from_playlist:
                            video_info = videos_data_from_playlist[video_id]
                            
                            # ID 추가
                            video_counter += 1
                            video_info['id'] = video_counter

                            # 전처리된 제목과 설명 추가
                            video_info['cleaned_title'] = clean_title(video_info['title'])
                            video_info['cleaned_description'] = clean_text_for_vectorization(video_info['description'])
                            
                            filtered_videos.append(video_info)
                            
            next_page_token = playlist_items_response.get('nextPageToken')
            if not next_page_token:
                print("모든 페이지를 탐색했습니다 (최신 영상부터 이전 영상까지).")
                break

    except Exception as e:
        print(f"API 요청 중 오류 발생: {e}")
        print("API 키가 유효한지, 채널 ID가 올바른지, 할당량이 남아있는지 확인해주세요.")
    
    return filtered_videos

# --- 메인 실행 부분 ---
if __name__ == '__main__':
    # --- 시작 시간 기록 ---
    start_time = time.time()

    # 중요: API_KEY를 실제 발급받은 값으로 변경했는지 다시 한번 확인해주세요!
    if API_KEY == "YOUR_ACTUAL_API_KEY_HERE":
        print("🚨 경고: API_KEY를 실제 발급받은 값으로 변경해주세요!")
        exit()

    # !!! 이 부분에 크롤링하려는 실제 YouTube 채널 URL을 입력하세요 !!!
    # KBS News 채널의 URL을 적용했습니다.
    youtube_url_input = "https://www.youtube.com/@newskbs" # 실제 KBS 뉴스 채널 URL로 수정

    print(f"\n대상 YouTube URL: {youtube_url_input}")
    print("\nURL에서 채널 ID를 추출하는 중...")
    extracted_channel_id = get_channel_id_from_youtube_url(API_KEY, youtube_url_input)

    if extracted_channel_id:
        print(f"추출된 채널 ID: {extracted_channel_id}")
        print("\nYouTube Data API를 사용하여 최근 영상 정보 가져오기 시작 (24시간 이내, 1~5분 길이)...")
        # hours=24 (24시간 이내), min_duration_sec=60 (1분), max_duration_sec=300 (5분)
        collected_videos = get_recent_videos_with_duration_filter(API_KEY, extracted_channel_id, hours=24, min_duration_sec=60, max_duration_sec=300)

        if collected_videos:
            print(f"\n총 {len(collected_videos)}개의 조건에 맞는 영상 정보를 가져왔습니다.")
            
            for i, video in enumerate(collected_videos):
                print(f"\n--- 영상 {i+1} ---")
                print(f"ID: {video['id']}") # 추가된 ID 출력
                print(f"제목: {video['title']}")
                print(f"클린 제목: {video['cleaned_title']}") # 추가된 클린 제목 출력
                print(f"설명: {video['description']}")
                print(f"클린 설명: {video['cleaned_description']}") # 추가된 클린 설명 출력
                print(f"링크: {video['link']}")
                print(f"업로드 시간: {video['publishedAt']}")
                print(f"썸네일 링크: {video['thumbnail_link']}")
                print("-" * 30)
            
            # 파일 이름에 채널 ID를 포함하여 저장 (파일 이름에 특수문자 방지)
            safe_channel_id = extracted_channel_id.replace('/', '_').replace('\\', '_')
            file_name = f"youtube_videos_filtered_{safe_channel_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            try:
                with open(file_name, 'w', encoding='utf-8') as f:
                    json.dump(collected_videos, f, ensure_ascii=False, indent=4)
                print(f"\n모든 필터링된 영상 정보를 '{file_name}' 파일에 JSON 형식으로 저장했습니다.")
            except Exception as e:
                print(f"파일 저장 중 오류 발생: {e}")
                print("수집된 영상 정보는 다음과 같습니다:")
                print(json.dumps(collected_videos, ensure_ascii=False, indent=4))

        else:
            print("조건에 맞는 최근 영상 정보를 찾을 수 없거나, 오류가 발생했습니다.")
    else:
        print("채널 ID를 추출하지 못하여 크롤링을 시작할 수 없습니다.")

    # --- 끝 시간 기록 및 총 걸린 시간 계산 ---
    end_time = time.time()
    total_time_seconds = end_time - start_time
    
    hours = int(total_time_seconds // 3600)
    minutes = int((total_time_seconds % 3600) // 60)
    seconds = total_time_seconds % 60
    
    print(f"\n총 실행 시간: {hours}시간 {minutes}분 {seconds:.2f}초")