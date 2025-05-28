import mysql.connector
import pandas as pd
from src.utils.config import *
import os
import datetime as dt
import logging
from typing import List
import numpy as np
logger = logging.getLogger('main_logger')


class GreyhoundData:
    def __init__(self):
        self.cnx = mysql.connector.connect(user='root',
                                   password=GREYHOUND_DB_PASSWD,
                                   host='localhost',
                                   database='mydb',
                                   port=3306)

    def get_greyhounds_full_data_dog_stats(self):
        '''
        Returns full history to calculate dog stats
        :return:
        '''
        logger.info('Enter mysql_interface.get_greyhounds_full_data_dog_stats')
        query = '''
        SELECT
        url,
        dog_name as dog_name,
        ifnull(DoB,0) as DoB,
        if(dog_gender="Female",0,if(dog_gender="Male",1,2)) as dog_genderB0D1,
        trainer
        from all_data_tab
        '''
        data = self.execute_scripts(query)
        basic_columns = ['url', 'dog_name', 'DoB', 'dog_genderB0D1', 'trainer']
        rslt = pd.DataFrame(data, columns=basic_columns)
        rslt = self.convert_types(rslt=rslt)
        logger.info(f'Extracted {len(rslt)} lines from the mysql database')
        return rslt

    def get_greyhounds_full_data_trainer_stats(self):
        '''
        Returns full history to calculate dog stats
        :return:
        '''
        logger.info('Enter mysql_interface.get_greyhounds_full_data_trainer_stats')
        query = '''
        SELECT 
        url,
        if(grade="OR",0,if(grade="OR1", 1, if(grade="OR2",2, if(grade="OR3", 3, right(grade, char_length(grade)-1))))) as grade,
        trap,
        all_data_tab.order,
        if(left(grade, 1) = "T", 1, 0) as is_trial,
        track,
        trainer,
        if(totalTime_readable+going_readable/100<0,"",distance / (totalTime_readable+going_readable/100)) as speed,
        totalTime_readable 
        from all_data_tab
        '''
        data = self.execute_scripts(query)
        basic_columns = ['url', 'grade', 'trap', 'order', 'is_trial', 'track', 'trainer', 'speed', 'totalTime_readable']
        rslt = pd.DataFrame(data, columns=basic_columns)
        rslt = self.convert_types(rslt=rslt)
        rslt = self.correct_data(rslt=rslt)
        logger.info(f'Extracted {len(rslt)} lines from the mysql database')
        return rslt

    def get_greyhounds_full_data_trap_stats(self):
        '''
        Returns full history to calculate trap stats
        :return:
        '''
        logger.info('Enter mysql_interface.get_greyhounds_full_data_trap_stats')
        query = '''
        SELECT 
        url,
        raceDate,
        if(grade="OR",0,if(grade="OR1", 1, if(grade="OR2",2, if(grade="OR3", 3, right(grade, char_length(grade)-1))))) as grade,
        trap,
        all_data_tab.order,
        if(left(grade, 1) = "T", 1, 0) as is_trial,
        track,
        if(totalTime_readable+going_readable/100<0,"",distance / (totalTime_readable+going_readable/100)) as speed,
        totalTime_readable
        from all_data_tab
        '''
        data = self.execute_scripts(query)
        basic_columns = ['url', 'raceDate', 'grade', 'trap', 'order', 'is_trial', 'track', 'speed', 'totalTime_readable']
        rslt = pd.DataFrame(data, columns=basic_columns)
        rslt = self.convert_types(rslt=rslt)
        rslt = self.correct_data(rslt=rslt)
        logger.info(f'Extracted {len(rslt)} lines from the mysql database')
        return rslt

    def get_greyhound_data(self, start_date: dt.date = None, end_date: dt.date=None, dog_list: List[str] = None):
        '''
        Runs query on mysql to source greyhound data and return it as dataframe
        :param start_date:
        :return:
        '''
        logger.info('Enter mysql_interface.get_greyhound_data')
        query = '''
        SET @uniqueKey=0;
        SELECT all_data_tab.url,
        @uniqueKey:=@uniqueKey+1 AS uniqueKey,
        raceDate,
        if(grade="OR",0,if(grade="OR1", 1, if(grade="OR2",2, if(grade="OR3", 3, right(grade, char_length(grade)-1))))) as grade,
        distance,
        dog_name as dog_name,
        trap,
        all_data_tab.order,
        ifnull(DoB,0) as DoB,
        totalTime_readable,
        secTime,
        if(totalTime_readable+going_readable/100<0,"",distance / (totalTime_readable+going_readable/100)) as speed,
        if(totalTime_readable+going_readable/100<0,"",distance / totalTime_readable) as speed_raw,
        weight_readable as weight,
        if(dog_gender="Female",0,if(dog_gender="Male",1,2)) as dog_genderB0D1,
        if(left(grade, 1) = "T", 1, 0) as is_trial,
        SP,
        ifnull(datediff(str_to_date(raceDate,"%Y-%m-%d"), str_to_date(DoB,"%b-%Y")+1)/365,0) as age,
        grade as grade_raw,
        raceDateTime,
        track,
        trainer,
        if (grade in ("OR", "OR1", "OR2", "OR3"), 1, 0) as is_or         
        from all_data_tab
        '''
        if start_date is not None and end_date is None:
            query += ' where raceDate >= "{}"'.format(start_date)

        if start_date is not None and end_date is not None:
            query += f' where raceDate >= "{start_date}" AND raceDate < "{end_date}"'
        if dog_list is not None:
            dog_list = ', '.join(f'"{d}"' for d in dog_list)
            if start_date is None and end_date is None:
                query += f' where url in (select distinct url from all_data_tab where dog_name in ({dog_list}))'
            else:
                query += f' and url in (select distinct url from all_data_tab where dog_name in ({dog_list}))'

        data = self.execute_scripts(query)
        basic_columns = ['url', 'uniqueKey', 'raceDate', 'grade', 'distance', 'dog_name', 'trap', 'order', 'DoB',
                         'totalTime_readable', 'secTime', 'speed', 'speed_raw', 'weight', 'dog_genderB0D1',
                         'is_trial', 'SP', 'age', 'grade_raw', 'raceDateTime', 'track', 'trainer', 'is_or']
        rslt = pd.DataFrame(data, columns=basic_columns)
        rslt = self.convert_types(rslt=rslt)
        rslt = self.correct_data(rslt=rslt)
        logger.info(f'Extracted {len(rslt)} lines from the mysql database')
        return rslt

    def get_speed_data(self, start_date: dt.date = None, end_date: dt.date=None, dog_list: List[str] = None):
        '''
        Runs query on mysql to source greyhound data and return it as dataframe
        :param start_date:
        :return:
        '''
        logger.info('Enter mysql_interface.get_speed_data')
        query = '''
        SELECT all_data_tab.url,
        raceDate,
        if(grade="OR",0,if(grade="OR1", 1, if(grade="OR2",2, if(grade="OR3", 3, right(grade, char_length(grade)-1))))) as grade,
        distance,
        dog_name as dog_name,
        trap,
        all_data_tab.order,
        totalTime_readable,
        secTime,
        if(totalTime_readable+going_readable/100<0,"",distance / (totalTime_readable+going_readable/100)) as speed,
        if(totalTime_readable<0,"",distance / (totalTime_readable)) as speed_raw,
        if(dog_gender="Female",0,if(dog_gender="Male",1,2)) as dog_genderB0D1,
        if(left(grade, 1) = "T", 1, 0) as is_trial,
        raceDateTime,
        track,
        going_readable as corr
        from all_data_tab
        '''
        if start_date is not None and end_date is None:
            query += ' where raceDate >= "{}"'.format(start_date)

        if start_date is not None and end_date is not None:
            query += f' where raceDate >= "{start_date}" AND raceDate < "{end_date}"'
        if dog_list is not None:
            dog_list = ', '.join(f'"{d}"' for d in dog_list)
            if start_date is None and end_date is None:
                query += f' where url in (select distinct url from all_data_tab where dog_name in ({dog_list}))'
            else:
                query += f' and url in (select distinct url from all_data_tab where dog_name in ({dog_list}))'

        data = self.execute_scripts(query)
        basic_columns = ['url', 'raceDate', 'grade', 'distance', 'dog_name', 'trap', 'order',
                         'totalTime_readable', 'secTime', 'speed', 'speed_raw', 'dog_genderB0D1',
                         'is_trial', 'raceDateTime', 'track', 'corr']
        rslt = pd.DataFrame(data, columns=basic_columns)
        for track in TRACKS:
            rslt[f'track_{track}'] = (rslt['track'] == track).astype(int)
        rslt = self.convert_types(rslt=rslt)
        logger.info(f'Extracted {len(rslt)} lines from the mysql database')
        return rslt

    def update_greyhounds_data(self):
        '''
        Transfers txt data into mysql database
        :return:
        '''
        logger.info('Update greyhound data starting')
        # build SQL query
        s1 = '''SET GLOBAL innodb_lock_wait_timeout = 5000;''' + '\n'

        s2 = '''truncate mydb.table_import;''' + '\n'

        if os.name == 'nt':
            gbgb_data_path = WINDOWS_GBGB_DATA_PATH
        else:
            gbgb_data_path = LINUX_GBGB_DATA_PATH
        s3 = f'''LOAD DATA INFILE "{gbgb_data_path}"
        INTO TABLE table_import
        CHARACTER SET latin1
        COLUMNS TERMINATED BY ','
        LINES TERMINATED BY "\\n"
        IGNORE 1 LINES;'''

        s4 = '''update table_import set raceDate=str_to_date(raceDate,'%d/%m/%Y');'''

        s5 = '''delete from all_data_tab where all_data_tab.url in (select url from table_import);''' + '\n'

        s6 = '''insert into all_data_tab(url,lineID,track,raceDate,raceDateTime,grade,distance,going_readable,all_data_tab.order,dog_name,trap,SP,totalTime_readable,weight_readable,DoB,dog_gender,dogID,secTime,Trainer)
        select 
        url,
        lineID,
        track,
        raceDate,
        raceDateTime,
        grade,
        distance,
        going_readable,
        table_import.order,
        dog_name,
        trap,
        SP,
        totalTime_readable,
        weight_readable,
        DoB,
        dog_gender,
        substring(dogID,2,char_length(dogID)-3),
        secTime,
        Trainer
        from table_import
        where table_import.url not in (select  distinct all_data_tab.url from all_data_tab)
        and  (if( (totalTime_readable+0)>0 ,(distance+0)/(totalTime_readable) , 0) > 0 OR  SP <> '' )
        And If( (totalTime_readable+0)>0 ,(distance+0)/(totalTime_readable) , 0) < 20
        and grade not in ('E1','E2','E3','GR','H1','H2','H3','H4','HD1','HD2','HD3','HD4','HP','HS1','HS2','HS3','HS4','IT','IV','KS','');'''

        for s in [s1, s2, s3, s4, s5, s6]:
            self.execute_scripts(query=s)
        logger.info('Finished to update greyhound data')
        logger.info('Delete todays data')
        self.delete_date(dt.datetime.now())
        logger.info('todays data deleted')

    def get_last_date(self):
        '''
        Returns last date available in database
        :return:
        '''
        s = "select Max(raceDate) as toPrint from all_data_tab;"
        return self.execute_scripts(query=s)[0]

    def delete_date(self, date):
        s = '''delete from all_data_tab where all_data_tab.raceDate="''' + date.strftime('%Y-%m-%d') + '''"'''
        self.execute_scripts(query=s)

    def execute_scripts(self, query):
        logger.info(f'Enter execute_scripts for {query}')
        sqlCommands = query.split(';')
        for command in sqlCommands:
            if command.strip() != '':
                cursor = self.cnx.cursor()
                cursor.execute(command)
                try:
                    data = cursor.fetchall()
                except:
                    data = None
                    self.cnx.commit()
                cursor.close()
        return data

    def convert_types(self, rslt):
        for v in ['speed', 'secTime', 'speed_raw', 'totalTime_readable', 'grade', 'weight', 'trap', 'age']:
            if v in rslt.columns.values:
                rslt[v] = pd.to_numeric(rslt[v], errors='coerce')
        for v, fill in zip(['order', 'url', 'corr', 'distance'], [0, 1, 0, 0]):
            if v in rslt.columns.values:
                rslt[v] = pd.to_numeric(rslt[v], errors='coerce').fillna(fill)
        if 'raceDate' in rslt.columns.values:
            rslt['raceDate'] = pd.to_datetime(rslt['raceDate'], format='%Y-%m-%d')
        if 'raceDateTime'in rslt.columns.values:
            rslt.raceDateTime = rslt.apply(lambda x: self.create_race_date_time(x.raceDate, x.raceDateTime), axis=1)
        return rslt

    def correct_data(self, rslt):
        rslt['is_error_in_row'] = np.where((rslt['order'] == 0), 1, 0)
        rslt['is_error_in_race'] = rslt['url'].map(rslt.groupby('url')['is_error_in_row'].sum())
        rslt = rslt[rslt['is_error_in_race'] == 0]
        rslt = rslt[~rslt['trap'].isna()]
        rslt = rslt[~rslt['grade'].isna()]
        rslt[rslt.track == 'Sittingbourne']['track'] = 'Central Park'
        rslt[rslt.track == 'Mildenhall']['track'] = 'Suffolk Downs'
        rslt = rslt[rslt.track.isin(TRACKS)]
        rslt['is_disqualified'] = np.where((rslt['totalTime_readable'] == 0) & (rslt['is_trial'] == 0), 1, 0)
        rslt = rslt[(rslt.speed > 0) | (rslt.is_disqualified == 1)]
        return rslt

    def create_race_date_time(self, raceDate, raceDateTime):
        try:
            raceDate = raceDate.replace(hour=int(raceDateTime[0:2]))
            raceDate = raceDate.replace(minute=int(raceDateTime[3:5]))
        except:
            try:
                raceDate = raceDate.replace(hour=12)
                raceDate = raceDate.replace(minute=0)
            except:
                pass
        return raceDate

if __name__ == '__main__':
    db = GreyhoundData()
    last_dt = db.get_last_date()
    #d=db.get_last_date()
    #db.delete_date(d[0])
    #print('done')
    db.update_greyhounds_data()
