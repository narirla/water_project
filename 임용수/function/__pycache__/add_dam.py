import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import numpy as np

class Add_Dam():
    def _prepare_date_columns(self, water_df, dam_df):
        """데이터 복사 및 날짜 형식 변환"""
        water_df = water_df.copy()
        # index가 날짜라고 가정 (원본 코드 유지)
        water_df['일자_dt'] = pd.to_datetime(water_df.index)
        
        dam_df = dam_df.copy()
        dam_df['일자'] = pd.to_datetime(dam_df['일자'])
        
        return water_df, dam_df
    
    def _aggregate_water_monthly(self, water_df):
        """수질 데이터: 연월별 '평균' 집계"""
        water_columns = {
            '수온': 'mean',
            '수소이온농도(ph)':'mean',
            '전기전도도(EC)':'mean',
            '용존산소(DO)':'mean',
            'BOD':'mean',
            'COD':'mean',
            '유량': 'mean',
            '총질소(T-N)':'mean',
            '총유기탄소(TOC)': 'mean',
            '총인(T-P)': 'mean',
            '부유물질': 'mean',
            '클로로필-a': 'mean'
        }
        #수온	수소이온농도(ph)	전기전도도(EC)	용존산소(DO)	BOD	COD	부유물질	총질소(T-N)	총인(T-P)	총유기탄소(TOC)	유량	클로로필-a
        # 존재하는 컬럼만 선택하여 집계 (에러 방지)
        available_water_cols = {k: v for k, v in water_columns.items() if k in water_df.columns}
        water_monthly = water_df.groupby(water_df['일자_dt'].dt.to_period('M')).agg(available_water_cols).reset_index()
        
        return water_monthly
    
    def _preprocess_dam_data(self, dam_df):
        """댐 데이터 전처리: 월 합계를 '일평균'으로 변환"""
        # 이미 월별로 한 행씩 있다고 가정하므로 바로 계산
        dam_df['days_in_month'] = dam_df['일자'].dt.daysinmonth
        dam_df['하굿둑방류량_평균'] = dam_df['하굿둑방류량'] / dam_df['days_in_month']
        dam_df['하굿둑강수량_평균'] = dam_df['하굿둑강수량'] / dam_df['days_in_month']
        
        # 병합을 위해 '일자'를 Period 형식으로 통일
        dam_df['일자_period'] = dam_df['일자'].dt.to_period('M')
        
        return dam_df
    
    def _merge_datasets(self, water_monthly, dam_df):
        """두 데이터 병합 (Period 기준)"""
        result = pd.merge(
            water_monthly, 
            dam_df[['일자_period', '하굿둑방류량_평균', '하굿둑강수량_평균']], 
            left_on='일자_dt', 
            right_on='일자_period', 
            how='inner'
        )
        
        return result
    
    def _finalize_result(self, result,new_order=['일자', '수온','수소이온농도(ph)','전기전도도(EC)','용존산소(DO)','BOD','COD','총질소(T-N)','유량','총유기탄소(TOC)','총인(T-P)','부유물질','하굿둑방류량_평균', '하굿둑강수량_평균', '클로로필-a']):
        """최종 정리: Period를 다시 Timestamp로 변환하고 불필요한 컬럼 삭제"""
        # Period를 다시 Timestamp로 변환하고 불필요한 컬럼 삭제
        result['일자'] = result['일자_dt'].dt.to_timestamp()
        result = result.drop(columns=['일자_dt', '일자_period'])
        
        # 정렬 및 결측치 처리
        result = result.sort_values('일자').reset_index(drop=True)
        result.dropna(inplace=True)
        
        return result.reindex(columns=new_order)
    
    def month_dam_add(self, water_df, dam_df):
        # 1. 데이터 복사 및 날짜 형식 변환
        water_df, dam_df = self._prepare_date_columns(water_df, dam_df)

        # 2. 수질 데이터: 연월별 '평균' 집계
        water_monthly = self._aggregate_water_monthly(water_df)

        # 3. 댐 데이터 전처리: 월 합계를 '일평균'으로 변환
        dam_df = self._preprocess_dam_data(dam_df)

        # 4. 두 데이터 병합 (Period 기준)
        result = self._merge_datasets(water_monthly, dam_df)

        # 5. 최종 정리: Period를 다시 Timestamp로 변환하고 불필요한 컬럼 삭제
        result = self._finalize_result(result)
        
        return result
    
    def month_dam_add_small(self, water_df, dam_df):
        # 1. 데이터 복사 및 날짜 형식 변환
        water_df, dam_df = self._prepare_date_columns(water_df, dam_df)

        # 2. 수질 데이터: 연월별 '평균' 집계
        water_monthly = self._aggregate_water_monthly(water_df)

        # 3. 댐 데이터 전처리: 월 합계를 '일평균'으로 변환
        dam_df = self._preprocess_dam_data(dam_df)

        # 4. 두 데이터 병합 (Period 기준)
        result = self._merge_datasets(water_monthly, dam_df)

        # 5. 최종 정리: Period를 다시 Timestamp로 변환하고 불필요한 컬럼 삭제
        result = self._finalize_result(result,new_order=[
            '일자',
            '수온', 
            '하굿둑방류량_평균', 
            '총인(T-P)', 
            '총질소(T-N)', 
            '하굿둑강수량_평균', 
            '클로로필-a' # Target
        ])
                
        return result

    def log_scale(self, df):
        # 1. 시계열 순서대로 정렬 (인덱스가 일자라면 인덱스 기준 정렬)
        # 인덱스가 datetime 형식이 아닐 수도 있으니 변환 후 정렬하는 것이 안전합니다.
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()

        # 2. 특정 컬럼 로그 변환 (원본 보존을 위해 copy 사용)
        df_transformed = df.copy()
        cols = ['유량', '하굿둑강수량_평균', '하굿둑방류량_평균','클로로필-a']
        for col in cols:
            if col in df_transformed.columns:
                df_transformed[col] = np.log1p(df_transformed[col])

        # 3. 독립변수(X)와 종속변수(y) 분리
        # y가 마지막 컬럼이라고 가정
        X = df_transformed.iloc[:, :-1]
        y = df_transformed.iloc[:, -1]

        # 4. 시계열 데이터 분할 (train_test_split 대신 순서대로 분할)
        # shuffle=False를 사용하면 train_test_split도 순서를 유지합니다.
        xtrain, xtest, ytrain, ytest = train_test_split(
            X, y, 
            test_size=0.2, 
            shuffle=False  # 시계열 데이터이므로 순서 섞지 않음
        )

        # 5. 스케일링
        sc = StandardScaler()
        # Train 데이터의 컬럼명과 인덱스를 보존하기 위해 다시 DataFrame화
        xtrain_scaled = pd.DataFrame(
            sc.fit_transform(xtrain), 
            columns=xtrain.columns, 
            index=xtrain.index
        )
        # Test 데이터는 Train의 기준으로 변환만 수행
        xtest_scaled = pd.DataFrame(
            sc.transform(xtest), 
            columns=xtest.columns, 
            index=xtest.index
        )

        return xtrain_scaled, xtest_scaled, ytrain, ytest
    