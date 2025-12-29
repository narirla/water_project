import pandas as pd

class Add_Dam():
    def month_dam_add(self, water_df, dam_df):
        # 1. 데이터 복사 및 날짜 형식 변환
        water_df = water_df.copy()
        # index가 날짜라고 가정 (원본 코드 유지)
        water_df['일자_dt'] = pd.to_datetime(water_df.index)
        
        dam_df = dam_df.copy()
        dam_df['일자'] = pd.to_datetime(dam_df['일자'])

        # 2. 수질 데이터: 연월별 '평균' 집계
        water_columns = {
            '수온': 'mean',
            '유량': 'mean',
            '총유기탄소(TOC)': 'mean',
            '총인(T-P)': 'mean',
            '부유물질': 'mean',
            '클로로필-a': 'mean'
        }
        # 존재하는 컬럼만 선택하여 집계 (에러 방지)
        available_water_cols = {k: v for k, v in water_columns.items() if k in water_df.columns}
        water_monthly = water_df.groupby(water_df['일자_dt'].dt.to_period('M')).agg(available_water_cols).reset_index()

        # 3. 댐 데이터 전처리: 월 합계를 '일평균'으로 변환
        # 이미 월별로 한 행씩 있다고 가정하므로 바로 계산
        dam_df['days_in_month'] = dam_df['일자'].dt.daysinmonth
        dam_df['하굿둑방류량_평균'] = dam_df['하굿둑방류량'] / dam_df['days_in_month']
        dam_df['하굿둑강수량_평균'] = dam_df['하굿둑강수량'] / dam_df['days_in_month']
        
        # 병합을 위해 '일자'를 Period 형식으로 통일
        dam_df['일자_period'] = dam_df['일자'].dt.to_period('M')

        # 4. 두 데이터 병합 (Period 기준)
        result = pd.merge(
            water_monthly, 
            dam_df[['일자_period', '하굿둑방류량_평균', '하굿둑강수량_평균']], 
            left_on='일자_dt', 
            right_on='일자_period', 
            how='inner'
        )

        # 5. 최종 정리: Period를 다시 Timestamp로 변환하고 불필요한 컬럼 삭제
        result['일자'] = result['일자_dt'].dt.to_timestamp()
        result = result.drop(columns=['일자_dt', '일자_period'])
        
        # 정렬 및 결측치 처리
        result = result.sort_values('일자').reset_index(drop=True)
        result.dropna(inplace=True)

        new_order = ['일자', '수온','유량','총유기탄소(TOC)','총인(T-P)','부유물질','하굿둑방류량_평균', '하굿둑강수량_평균', '클로로필-a']
        
        return result.reindex(columns=new_order)