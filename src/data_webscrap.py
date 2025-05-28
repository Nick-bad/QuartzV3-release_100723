import requests
import pandas as pd
import queue
from threading import Thread
import sys
import os
import logging
from typing import List
from src.utils.config import *
from src.utils.mysql_interface import GreyhoundData
from src.utils.utils import exception_handler
import datetime as dt
from src.utils.emails_interface import MailGunEmail
logger = logging.getLogger('main_logger')


class GBGB_webscrapper:

    def __init__(self):
        if os.name == 'nt':
            self.gbgb_data_path = WINDOWS_GBGB_DATA_PATH
        else:
            self.gbgb_data_path = LINUX_GBGB_DATA_PATH

    def scrap_meeting(self, meeting_id) -> pd.DataFrame:
        req = requests.get('https://api.gbgb.org.uk/api/results/meeting/'
                           + str(meeting_id)
                           + '?meeting='
                           + str(meeting_id))
        req = req.json()

        data = []
        cols = ['url', 'lineID', 'track', 'raceDate', 'raceDateTime', 'grade', 'distance',
                'going', 'going_readable', 'order', 'dog_name', 'trap', 'SP', 'secTime',
                'totalTime', 'totalTime_readable', 'weight', 'weight_readable', 'DoB',
                'dog_gender', 'Trainer', 'dogID']

        try:
            for race in req[0]['races']:
                try:
                    going = race['raceGoing']
                except:
                    going = 0

                for trap in race['traps']:
                    try:
                        try:
                            dog_sex = '-'
                            if trap['dogSex'] == 'b':
                                dog_sex = 'Female'
                            if trap['dogSex'] == 'd':
                                dog_sex = 'Male'
                        except:
                            dog_sex = '-'

                        try:
                            dog_born = trap['dogBorn']
                        except:
                            dog_born = ''

                        try:
                            trainer_name = trap['trainerName']
                        except:
                            trainer_name = ''

                        try:
                            weight = trap['resultDogWeight']
                        except:
                            weight = ''

                        try:
                            sec_time = trap['resultSectionalTime']
                        except:
                            sec_time = ''

                        try:
                            sp = trap['SP']
                        except:
                            sp = ''

                        row = [str(meeting_id) + str('000') + str(race['raceId']),
                               str(meeting_id) + str('000') + str(race['raceId']) + str(trap['trapNumber']),
                               req[0]['trackName'],
                               req[0]['meetingDate'],
                               race['raceTime'],
                               race['raceClass'],
                               race['raceDistance'],
                               going,
                               going,
                               trap['resultPosition'],
                               trap['dogName'],
                               trap['trapNumber'],
                               sp,
                               sec_time,
                               trap['resultRunTime'],
                               trap['resultRunTime'],
                               weight,
                               weight,
                               dog_born,
                               dog_sex,
                               trainer_name,
                               trap['dogId']]
                        data.append(row)
                    except:
                        pass
        except:
            pass

        df = pd.DataFrame(data, columns=cols)
        df['going'].fillna(0, inplace=True)
        df['going_readable'].fillna(0, inplace=True)
        df = df[(~df.totalTime.isna()) | ((df.totalTime.isna()) & (df.SP != ''))]
        return df

    def scrap_n_meetings(self, meeting_id_list) -> List[pd.DataFrame]:
        '''
        meeting_id_list: list of ids to scrap
        '''
        que = queue.Queue()
        threads_list = list()
        for meeting_id in meeting_id_list:
            t = Thread(target=lambda q, arg1: q.put(self.scrap_meeting(arg1)), args=(que, meeting_id))
            t.start()
            threads_list.append(t)

        for t in threads_list:
            t.join()

        df_list = list()
        while not que.empty():
            df_list.append(que.get())
        return df_list

    def scrap_all_meetings(self, meeting_id_start, meeting_id_end, n):
        '''
        Scraps GBGB website and stores data in txt folder
        n: number of parallel jobs
        '''
        meeting_id_list = [i for i in range(meeting_id_start, meeting_id_end + 1)]
        meeting_id_list_of_sublists = [meeting_id_list[i:i + n] for i in range(0, len(meeting_id_list), n)]
        dfs = [self.scrap_n_meetings(meeting_id_sublist) for meeting_id_sublist in meeting_id_list_of_sublists]
        df_list = [df for df_ in dfs for df in df_]
        df = pd.concat(df_list, axis=0)
        df.to_csv(self.gbgb_data_path, header=True, index=False)

    def get_latest_meeting_ids(self):
        req_1 = requests.get('https://api.gbgb.org.uk/api/results?page=1&itemsPerPage=20&race_type=race')
        req_2 = requests.get('https://api.gbgb.org.uk/api/results?page=1&itemsPerPage=20')
        req_1 = pd.DataFrame(req_1.json()['items'])
        req_2 = pd.DataFrame(req_2.json()['items'])

        return [min(req_1['meetingId'].min(), req_2['meetingId'].min()),
                max(req_1['meetingId'].max(), req_2['meetingId'].max())]

    @exception_handler
    def update_db(self, start=None, end=None):
        '''
        Webscraps data. If start and end not provided source boundaries from GBGB website. Updates mysql.
        :param start:
        :param end:
        :return:
        '''
        db = GreyhoundData()
        if start is None or end is None:
            start, end = self.get_latest_meeting_ids()
        logger.info(f'Scrapping meetings from {start} to {end}')
        self.scrap_all_meetings(meeting_id_start=start - 2000, meeting_id_end=end + 2000, n=100)
        logger.info('Data scrapped. Updating mysql.')
        db.update_greyhounds_data()
        logger.info('Mysql updated')
        # check the last date updated is yesterday
        last_dt = db.get_last_date()[0]
        mg = MailGunEmail(env=ENV)
        if last_dt == dt.date.today() - dt.timedelta(1):
            mg.send_text_email(subject='Greyhounds Data base updated OK',
                               text=f'Last updated date in db is {last_dt.strftime(format="%d-%m-%Y")}')
        else:
            mg.send_text_email(subject='Issue with Greyhounds database update',
                               text=f'Last updated date in db is {last_dt.strftime(format="%d-%m-%Y")}')

def main():
    ''' use this main to populate a db from scratch '''
    # change path on Linux
    if os.name != 'nt':
        os.chdir('/home/nicolas/Documents/tipstronic_backend-master/')
    ws = GBGB_webscrapper()
    ws.update_db(330000, 330200)
    print('load db from scratch: initial test done')
    ws.update_db(280000, 300000)
    ws.update_db(300000, 330000)
    ws.update_db(330000, 360000)
    ws.update_db(360000, 365000)
    ws = GBGB_webscrapper()
    ws.update_db()
    ws.update_db(386000, 396000)

if __name__ == "__main__":
    sys.exit(int(main() or 0))
    print('it is finished')