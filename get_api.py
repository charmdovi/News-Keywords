from airflow.models.variable import Variable
from datetime import datetime, timedelta
import pandas as pd
import requests
from mysql import connector
import json

APIKEY = Variable.get("API_KEY")

class MovieDataUploader:

    def input(self):
        # 사용자로부터 시작일과 종료일을 입력 받음
        input_start = input("조회를 원하는 시작일을 설정해주세요. (yyyymmdd): ")
        input_end = input("조회 마지막 날짜를 설정해주세요. (yyyymmdd): ")
        return input_start, input_end
    
    def date_range(self, start, end):
        dates = pd.date_range(start=start, end=end)
        dates = [date.strftime('%Y%m%d') for date in dates]
        return dates
    

    def upload_movie_data(self):
        start, end = self.input()
        dates = self.date_range(start, end)
        int_dates = [int(i) for i in dates]

        for num in int_dates:
            url = f"https://newsapi.org/v2/top-headlines?country=kr&apiKey={APIKEY}"
            response = requests.get(url)
            output = response.json()

            sum_sales = 0
            sum_audi = 0
            for i in range(10):
                salesAmt = output['boxOfficeResult']['dailyBoxOfficeList'][i]['salesAmt']
                audiCnt = output['boxOfficeResult']['dailyBoxOfficeList'][i]['audiCnt']
                sum_sales = sum_sales + int(salesAmt)
                sum_audi = sum_audi + int(audiCnt)

            sql = f"INSERT into movie (date, sales_amt, audi_cnt) \
                    values ({num}, {sum_sales}, {sum_audi})"

            connection_result = self.conn_db()
            cursor = connection_result.cursor()
            cursor.execute(sql)
            connection_result.commit()
            connection_result.close()

        print(f"{dates[0]}~{dates[-1]} 해당 기간의 자료가 업로드 완료 되었습니다.")



HOST = '3.35.3.71'
USER = 'myname'
PW = '1234'
DB = 'mydb'
PORT = '50693'

uploader = MovieDataUploader(HOST, PORT, USER, PW, DB)
uploader.upload_movie_data()

