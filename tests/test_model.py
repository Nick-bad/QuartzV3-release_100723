import unittest
import pandas as pd
from typing import List
import src.utils.mysql_interface
from tests.config import *
from src.utils.utils import localize
from src.day_manager import DayManager
from unittest.mock import patch
from src.utils.mysql_interface import GreyhoundData
from src.betfair_source import Betfair


def mock_get_greyhounds_full_data_dog_stats(self):
    '''
    Returns full history to calculate dog stats
    :return:
    '''
    query = '''
    SELECT
    url,
    dog_name as dog_name,
    ifnull(DoB,0) as DoB,
    if(dog_gender="Female",0,if(dog_gender="Male",1,2)) as dog_genderB0D1,
    trainer
    from all_data_tab where all_data_tab.raceDate <= "2023-06-23"
    '''
    data = self.execute_scripts(query)
    basic_columns = ['url', 'dog_name', 'DoB', 'dog_genderB0D1', 'trainer']
    rslt = pd.DataFrame(data, columns=basic_columns)
    rslt = self.convert_types(rslt=rslt)
    return rslt

def mock_get_greyhounds_full_data_trainer_stats(self):
    '''
    Returns full history to calculate dog stats
    :return:
    '''
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
    from all_data_tab where all_data_tab.raceDate <= "2023-06-23"
    '''
    data = self.execute_scripts(query)
    basic_columns = ['url', 'grade', 'trap', 'order', 'is_trial', 'track', 'trainer', 'speed', 'totalTime_readable']
    rslt = pd.DataFrame(data, columns=basic_columns)
    rslt = self.convert_types(rslt=rslt)
    rslt = self.correct_data(rslt=rslt)
    return rslt

def mock_get_greyhounds_full_data_trap_stats(self):
    '''
    Returns full history to calculate trap stats
    :return:
    '''
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
    from all_data_tab where all_data_tab.raceDate <= "2023-06-23"
    '''
    data = self.execute_scripts(query)
    basic_columns = ['url', 'raceDate', 'grade', 'trap', 'order', 'is_trial', 'track', 'speed', 'totalTime_readable']
    rslt = pd.DataFrame(data, columns=basic_columns)
    rslt = self.convert_types(rslt=rslt)
    rslt = self.correct_data(rslt=rslt)
    return rslt

def mock_get_greyhound_data(self, start_date: dt.date = None, end_date: dt.date = None, dog_list: List[str] = None):
    '''
    Runs query on mysql to source greyhound data and return it as dataframe
    :param start_date:
    :return:
    '''
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
    from all_data_tab where all_data_tab.raceDate <= "2023-06-23"
    '''
    if dog_list is not None:
        dog_list = ', '.join(f'"{d}"' for d in dog_list)
        query += f' and url in (select distinct url from all_data_tab where dog_name in ({dog_list}))'
    data = self.execute_scripts(query)
    basic_columns = ['url', 'uniqueKey', 'raceDate', 'grade', 'distance', 'dog_name', 'trap', 'order', 'DoB',
                     'totalTime_readable', 'secTime', 'speed', 'speed_raw', 'weight', 'dog_genderB0D1',
                     'is_trial', 'SP', 'age', 'grade_raw', 'raceDateTime', 'track', 'trainer', 'is_or']
    rslt = pd.DataFrame(data, columns=basic_columns)
    rslt = self.convert_types(rslt=rslt)
    rslt = self.correct_data(rslt=rslt)
    return rslt

def mock_get_speed_data(self, start_date: dt.date = None, end_date: dt.date = None, dog_list: List[str] = None):
    '''
    Runs query on mysql to source greyhound data and return it as dataframe
    :param start_date:
    :return:
    '''
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
    from all_data_tab where all_data_tab.raceDate <= "2023-06-23"
    '''

    if dog_list is not None:
        dog_list = ', '.join(f'"{d}"' for d in dog_list)
        query += f' and url in (select distinct url from all_data_tab where dog_name in ({dog_list}))'

    data = self.execute_scripts(query)
    basic_columns = ['url', 'raceDate', 'grade', 'distance', 'dog_name', 'trap', 'order',
                     'totalTime_readable', 'secTime', 'speed', 'speed_raw', 'dog_genderB0D1',
                     'is_trial', 'raceDateTime', 'track', 'corr']
    rslt = pd.DataFrame(data, columns=basic_columns)
    for track in TRACKS:
        rslt[f'track_{track}'] = (rslt['track'] == track).astype(int)
    rslt = self.convert_types(rslt=rslt)
    return rslt

def mock_get_market_description(self, market_id):
    df = pd.DataFrame(data=[r[0: 2] for r in RACES[market_id]['runners']], columns=['trap', 'dog_name'])
    df['market_id'] = str(market_id)
    df['track'] = RACES[market_id]['track']
    df['start_time_utc'] = RACES[market_id]['race_dt']
    df['selection_id'] = df['trap']
    df['status'] = 'ACTIVE'
    df['grade_raw'] = 'dummy'
    df['grade'] = RACES[market_id]['grade']
    df['distance'] = RACES[market_id]['distance']
    df['start_time_loc'] = localize(RACES[market_id]['race_dt'])
    df['corr'] = RACES[market_id]['corr']
    df['is_or'] = RACES[market_id]['is_or']
    return df[['market_id', 'track', 'start_time_utc', 'selection_id', 'status',
               'grade_raw', 'grade', 'distance', 'dog_name', 'trap', 'start_time_loc', 'corr', 'is_or']]


class TestModel(unittest.TestCase):

    @patch('src.utils.mysql_interface.GreyhoundData.get_greyhounds_full_data_dog_stats', mock_get_greyhounds_full_data_dog_stats)
    @patch('src.utils.mysql_interface.GreyhoundData.get_greyhounds_full_data_trap_stats', mock_get_greyhounds_full_data_trap_stats)
    @patch('src.utils.mysql_interface.GreyhoundData.get_greyhounds_full_data_trainer_stats', mock_get_greyhounds_full_data_trainer_stats)
    @patch('src.utils.mysql_interface.GreyhoundData.get_greyhound_data', mock_get_greyhound_data)
    @patch('src.utils.mysql_interface.GreyhoundData.get_speed_data', mock_get_speed_data)
    @patch('src.betfair_source.Betfair.get_market_description', mock_get_market_description)
    def test_quartz_calcs(self):
        day_manager = DayManager(env='UAT')
        # execute test
        day_manager.model.calculate_static_data()
        rslt = {}
        for market_id in ['1.000']:
            quartz = day_manager.model.calculate_quartz(market_id=market_id)
            quartz = dict(quartz[['dog_name', 'quartz_pred']].values)
            #for dog in quartz:
                #self.assertEqual(str(quartz[dog])[0: 7], str(EXP_QUARTZ[dog])[0: 7])
