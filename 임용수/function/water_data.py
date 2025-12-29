"""Water data fetching and processing module."""
import pandas as pd
import requests
from typing import Optional
from secret.key import Key


class Water:
    """Class for fetching and processing water quality data from APIs."""
    
    # API Configuration
    WATER_API_URL = 'https://apis.data.go.kr/1480523/WaterQualityService/getWaterMeasuringList'
    DAM_API_URL = 'https://api.odcloud.kr/api/15143064/v1/uddi:62b4a7c0-558f-4117-baec-f75e3e95b1e3'
    
    # Default API parameters
    DEFAULT_PAGE_NO = '1'
    DEFAULT_NUM_OF_ROWS = '3000'
    DEFAULT_RESULT_TYPE = 'json'
    DEFAULT_PT_NO_LIST = '2022A30,2022A10'
    DEFAULT_WMYR_LIST = '2021,2022,2023,2024,2025'
    DEFAULT_WMOD_LIST = '01,02,03,04,05,06,07,08,09,10,11,12'
    
    # Column mapping for water quality data
    RENAME_MAP = {
        'PT_NM': '총량지점명',
        'WMCYMD': '일자',
        'ITEM_TEMP': '수온',  # 단위: ℃
        'ITEM_PH': '수소이온농도(ph)',
        'ITEM_EC': '전기전도도(EC)',  # 단위: μS/㎝
        'ITEM_DOC': '용존산소(DO)',  # 단위: ㎎/L
        'ITEM_BOD': 'BOD',  # 단위: ㎎/L
        'ITEM_COD': 'COD',  # 단위: ㎎/L
        'ITEM_SS': '부유물질',  # 단위: ㎎/L
        'ITEM_TN': '총질소(T-N)',  # 단위: ㎎/L
        'ITEM_TP': '총인(T-P)',  # 단위: ㎎/L
        'ITEM_TOC': '총유기탄소(TOC)',  # 단위: ㎎/L
        'ITEM_AMNT': '유량',  # 단위: ㎥/s
        'ITEM_CLOA': '클로로필-a'
    }
    
    # Numeric columns (all except '총량지점명' and '일자')
    NUMERIC_COLUMNS = [
        '수온', '수소이온농도(ph)', '전기전도도(EC)', '용존산소(DO)', 
        'BOD', 'COD', '부유물질', '총질소(T-N)', '총인(T-P)', 
        '총유기탄소(TOC)', '유량', '클로로필-a'
    ]
    
    # Dam data column mapping
    DAM_RENAME_MAP = {
        '날짜': '일자',
        '방류량(백만톤)': '하굿둑방류량',
        '강수량(밀리미터)': '하굿둑강수량'
    }
    
    def __init__(self) -> None:
        """Initialize Water class with API keys."""
        key = Key()
        self.key = key.water_api_key
        self.dam_key = key.dam_api_key
    
    def total_water(self) -> pd.DataFrame:
        """
        Load total water quantity measurement data from CSV file.
        
        Returns:
            pd.DataFrame: DataFrame with water quantity data, with '일자' column 
                         converted to datetime format.
        """
        df = pd.read_csv('data/total_water_quantity_measurement.csv', encoding='euc-kr')
        df['일자'] = pd.to_datetime(df['일자'], errors='coerce')
        return df
    
    def _fetch_water_api_data(self, rename_columns: bool = True) -> pd.DataFrame:
        """
        Internal method to fetch water quality data from API.
        
        Args:
            rename_columns: If True, rename columns using RENAME_MAP and process data.
                          If False, return raw data without renaming.
        
        Returns:
            pd.DataFrame: DataFrame containing water quality data.
        """
        params = {
            'serviceKey': self.key,
            'pageNo': self.DEFAULT_PAGE_NO,
            'numOfRows': self.DEFAULT_NUM_OF_ROWS,
            'resultType': self.DEFAULT_RESULT_TYPE,
            'ptNoList': self.DEFAULT_PT_NO_LIST,
            'wmyrList': self.DEFAULT_WMYR_LIST,
            'wmodList': self.DEFAULT_WMOD_LIST
        }
        
        try:
            response = requests.get(self.WATER_API_URL, params=params, verify=True, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            items = data.get('getWaterMeasuringList', {}).get('item', [])
            
            if not items:
                print("응답은 성공했으나 데이터가 없습니다.")
                return pd.DataFrame()
            
            df = pd.DataFrame(items)
            
            if rename_columns:
                # Extract and rename columns
                df = df[list(self.RENAME_MAP.keys())].copy()
                df.rename(columns=self.RENAME_MAP, inplace=True)
                
                # Convert numeric columns
                df[self.NUMERIC_COLUMNS] = df[self.NUMERIC_COLUMNS].apply(
                    pd.to_numeric, errors='coerce'
                )
                
                # Convert date column
                df['일자'] = pd.to_datetime(df['일자'], errors='coerce')
                
                print("--- 분석 준비 완료: 핵심 수질 지표 ---")
                print(df.head())
            
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"API 요청 실패: {e}")
        except (KeyError, ValueError) as e:
            print(f"데이터 처리 오류: {e}")
        except Exception as e:
            print(f"오류 발생: {e}")
        
        return pd.DataFrame()
    
    def api_data(self) -> pd.DataFrame:
        """
        Fetch water quality data from API with column renaming and data processing.
        
        Returns:
            pd.DataFrame: Processed DataFrame with renamed columns and converted data types.
        """
        return self._fetch_water_api_data(rename_columns=True)
    
    def api_data_dept(self) -> pd.DataFrame:
        """
        Fetch raw water quality data from API without column renaming.
        
        Returns:
            pd.DataFrame: Raw DataFrame without column renaming or data type conversion.
        """
        return self._fetch_water_api_data(rename_columns=False)
    
    def dam(self) -> pd.DataFrame:
        """
        Fetch dam discharge and rainfall data from API.
        
        Returns:
            pd.DataFrame: DataFrame containing dam data with columns:
                         - 일자: Date
                         - 하굿둑방류량: Discharge amount (million tons)
                         - 하굿둑강수량: Rainfall (millimeters)
        """
        params = {
            'page': 1,
            'perPage': 2000,
            'serviceKey': self.dam_key,
            'returnType': 'JSON'
        }
        
        try:
            response = requests.get(self.DAM_API_URL, params=params, verify=True, timeout=30)
            response.raise_for_status()
            
            res_json = response.json()
            items = res_json.get('data', [])
            
            if not items:
                match_count = res_json.get('matchCount', 0)
                print(f"API 호출 성공했으나 데이터가 0건임. (matchCount: {match_count})")
                return pd.DataFrame()
            
            df = pd.DataFrame(items)
            
            # Rename columns
            df.rename(columns=self.DAM_RENAME_MAP, inplace=True)
            
            # Convert date column
            df['일자'] = pd.to_datetime(df['일자'], errors='coerce').dt.normalize()
            
            # Convert numeric columns
            df['하굿둑방류량'] = pd.to_numeric(df['하굿둑방류량'], errors='coerce')
            df['하굿둑강수량'] = pd.to_numeric(df['하굿둑강수량'], errors='coerce')
            
            print(f"하굿둑 데이터 확보 성공: {len(df)}건")
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"하굿둑 API 요청 실패: {e}")
        except (KeyError, ValueError) as e:
            print(f"하굿둑 데이터 처리 오류: {e}")
        except Exception as e:
            print(f"하굿둑 API 에러: {e}")
        
        return pd.DataFrame()
