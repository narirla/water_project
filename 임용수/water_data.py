import pandas as pd
from secret.key import Key
import requests

class Water():
    def __init__(self):
        k=Key()
        self.url = 'https://apis.data.go.kr/1480523/WaterQualityService/getWaterMeasuringList'
        self.url_dam='https://infuser.odcloud.kr/oas/docs?namespace=15143064/v1'
        self.key=k.water_api_key
        self.dam_key=k.dam_api_key
        self.rename_map = {
                        'PT_NM': '총량지점명',
                        'WMCYMD': '일자',
                        'ITEM_TEMP': '수온', # 단위 : ℃
                        'ITEM_PH': '수소이온농도(ph)',
                        'ITEM_EC': '전기전도도(EC)',    # 단위 : μS/㎝
                        'ITEM_DOC': '용존산소(DO)', # 단위 : ㎎/L
                        'ITEM_BOD': 'BOD',  # 단위 : ㎎/L
                        'ITEM_COD': 'COD',  # 단위 : ㎎/L
                        'ITEM_SS': '부유물질',  # 단위 : ㎎/L
                        'ITEM_TN': '총질소(T-N)',   # 단위 : ㎎/L
                        'ITEM_TP': '총인(T-P)', # 단위 : ㎎/L
                        'ITEM_TOC': '총유기탄소(TOC)',  # 단위 : ㎎/L
                        'ITEM_AMNT': '유량',    # 단위 : ㎥/s
                        'ITEM_CLOA': '클로로필-a'
                    }

    # 클래스 메서드로 선언하거나 인스턴스 메서드로 유지 (self 추가 권장)
    def total_water(self):
        df2 = pd.read_csv('data/total_water_quantity_measurement.csv', encoding='euc-kr')
        df2['일자'] = pd.to_datetime(df2['일자'], errors='coerce')
        return df2
    
    def api_data(self):
        params = {
            'serviceKey': self.key,
            'pageNo': '1',
            'numOfRows': '3000',
            'resultType': 'json',
            'ptNoList': '2022A30,2022A10',
            'wmyrList': '2021,2022,2023,2024,2025',
            'wmodList': '01,02,03,04,05,06,07,08,09,10,11,12'
        }

        # 1. 결과물을 담을 변수를 미리 빈 값으로 초기화
        waterDF = pd.DataFrame() 

        try:
            response = requests.get(self.url, params=params, verify=True)
            
            if response.status_code == 200:
                data = response.json()
                
                # 4. 데이터 추출 (구조: getWaterMeasuringList -> item)
                items = data.get('getWaterMeasuringList', {}).get('item', [])
                
                if items:
                    df = pd.DataFrame(items)
                    # df.set_index('ROWNO',inplace=True)

                    
                    
                    # 2. 필요한 컬럼만 추출하여 새 DF 생성
                    waterDF = df[list(self.rename_map.keys())].copy()
                    waterDF.rename(columns=self.rename_map, inplace=True)

                    # 3. 데이터 타입 숫자형으로 변환 (연산 가능하게)
                    num_cols = ['수온', '수소이온농도(ph)', '전기전도도(EC)', '용존산소(DO)', 'BOD', 'COD', '부유물질', '총질소(T-N)', '총인(T-P)', '총유기탄소(TOC)', '유량','클로로필-a']
                    waterDF[num_cols] = waterDF[num_cols].apply(pd.to_numeric, errors='coerce')

                    # # 4. 날짜 데이터 형식 변환 (YYYYMMDD -> datetime)
                    waterDF['일자'] = pd.to_datetime(waterDF['일자'], errors='coerce')

                    print("--- 분석 준비 완료: 핵심 수질 지표 ---")
                    print(waterDF.head())
                else:
                    print("응답은 성공했으나 데이터가 없습니다.")
            else:
                print(f"API 요청 실패: {response.status_code}")

        except Exception as e:
            print(f"오류 발생: {e}")
        return waterDF
    def api_data_dept(self):
        params = {
            'serviceKey': self.key,
            'pageNo': '1',
            'numOfRows': '3000',
            'resultType': 'json',
            'ptNoList': '2022A30,2022A10',
            'wmyrList': '2021,2022,2023,2024,2025',
            'wmodList': '01,02,03,04,05,06,07,08,09,10,11,12'
        }

        # 1. 결과물을 담을 변수를 미리 빈 값으로 초기화
        waterDF = pd.DataFrame() 

        try:
            response = requests.get(self.url, params=params, verify=True)
            
            if response.status_code == 200:
                data = response.json()
                
                # 4. 데이터 추출 (구조: getWaterMeasuringList -> item)
                items = data.get('getWaterMeasuringList', {}).get('item', [])
                
                if items:
                    df = pd.DataFrame(items)
                    # df.set_index('ROWNO',inplace=True)

                    
                    
                    # 2. 필요한 컬럼만 추출하여 새 DF 생성
                    waterDF = df.copy()
                    
                else:
                    print("응답은 성공했으나 데이터가 없습니다.")
            else:
                print(f"API 요청 실패: {response.status_code}")

        except Exception as e:
            print(f"오류 발생: {e}")
        return waterDF
    def dam(self):
        # 명세서 기준 정확한 경로와 파라미터
        url = "https://api.odcloud.kr/api/15143064/v1/uddi:62b4a7c0-558f-4117-baec-f75e3e95b1e3"
        
        params = {
            'page': 1,
            'perPage': 2000, # 전체 데이터를 한 번에 가져오기 위해 크게 설정
            'serviceKey': self.dam_key,
            'returnType': 'JSON'
        }

        try:
            response = requests.get(url, params=params, verify=True)
            # 만약 401 에러가 난다면 아래처럼 헤더에 키를 넣어보세요.
            # headers = {'Authorization': f'Infuser {self.dam_key}'}
            # response = requests.get(url, params=params, headers=headers)

            if response.status_code == 200:
                res_json = response.json()
                items = res_json.get('data', [])
                
                if items:
                    df = pd.DataFrame(items)
                    # 컬럼명 매핑 (명세서 기준)
                    df = df.rename(columns={
                        '날짜': '일자',
                        '방류량(백만톤)': '하굿둑방류량',
                        '강수량(밀리미터)': '하굿둑강수량'
                    })
                    
                    # 전처리: 날짜 형식 맞추기 (수질 데이터와 join용)
                    df['일자'] = pd.to_datetime(df['일자']).dt.normalize()
                    # 수치 변환 (문자열로 들어올 경우 대비)
                    df['하굿둑방류량'] = pd.to_numeric(df['하굿둑방류량'], errors='coerce')
                    
                    print(f"하굿둑 데이터 확보 성공: {len(df)}건")
                    return df
                else:
                    # 여기에 도달한다면 API 키 권한은 있으나 결과셋이 0인 상태입니다.
                    print(f"API 호출 성공했으나 데이터가 0건임. (matchCount: {res_json.get('matchCount')})")
        except Exception as e:
            print(f"하굿둑 API 에러: {e}")
        
        return pd.DataFrame()