from src.utils.config import *
import src.utils.mysql_interface as db
import pandas as pd
from pandas.core.window.rolling import RollingAndExpandingMixin
import numpy as np
from src.utils.utils import *
import datetime as dt
from typing import Dict
import pickle as pkl
import joblib
import logging
logger = logging.getLogger('main_logger')


class GreyhoundsModel:

    def __init__(self, data_source):
        '''
        Model coupled with datasource
        Most parts of code obfuscated
        '''
        self.db_conn = db.GreyhoundData()
        self.data_source = data_source
        self.gbm_1 = joblib.load(GREYHOUND_LGBM_1)
        self.features_1 = pkl.load(open(GREYHOUND_LGBM_FEATURES_1, 'rb'))


    def calculate_static_data(self) -> None:
        '''
        Get dog and trainer stats in the morning
        :return:
        '''
        logger.info('start calculating static data')
        self.dog_static_data = self.get_dog_static_data()
        logger.info('start calculating trap stats')
        self.trap_stats = self.calculate_trap_stats()
        logger.info('start calculating trainer stats')
        self.trainer_stats = self.calculate_trainer_stats()
        logger.info('finished calculating static data')

    def calculate_quartz(self, market_id: str):
        logger.info('Calculate Quartz score')
        df = self.get_current_racecard_with_features(market_id=market_id)
        if ENV == 'UAT':
            df[['dog_name'] + self.features_1].to_csv(f'src/utils/assets/debug/features_v1_{market_id[2:]}_jan23.csv')
        assert len(df[df['DoB'].isin([0, '0'])]) == 0, FEATURE_MISSING_MESSAGE + ' : DoB'
        assert len(df[df['dog_genderB0D1'].isin([2, '2'])]) == 0, FEATURE_MISSING_MESSAGE + ' : gender'
        assert len(df[df['age'] < 0.001]) == 0, FEATURE_MISSING_MESSAGE + ' : age'
        for feature in self.features_1:
            assert len(df[df[feature].isna()]) == 0, FEATURE_MISSING_MESSAGE + f' : {feature}'
        # run LGBM model
        df['quartz_pred'] = self.gbm_1.predict(df[self.features_1])
        logger.info('Finished to calculate Quartz score')
        return df

    def calculate_features(self, rslt, rslt_speed_ratings) -> pd.DataFrame:
        '''
        Adds features to rslt
        :param rslt:
        :return:
        '''
        raise NotImplementedError

    def calculate_speed_rating(self, rslt) -> pd.DataFrame:
        raise NotImplementedError

    def calculate_speed_rating_features(self, rslt):
        raise NotImplementedError

    def pivot_speed_rating_features(self, rslt):
        raise NotImplementedError

    def map_track_effect(self, x):
        if f'track_{x}' in self.track_coeffs:
            return self.track_coeffs.get(f'track_{x}')
        else:
            return np.nan

    def calculate_trap_stats(self) -> Dict:
        '''
        :param rslt: dataframe with entire histo
        :return: trap stats in dict
        :can be ran once a day only at start of the day
        '''
        rslt_stats = self.db_conn.get_greyhounds_full_data_trap_stats()
        rslt_stats['is_winner'] = np.where(rslt_stats['order'] == 1, 1, 0)
        rslt_stats['track_trap'] = rslt_stats['track'].astype(str) + '-' + rslt_stats['trap'].astype(str)
        trap_stats = rslt_stats[['raceDate', 'track', 'trap', 'track_trap', 'is_winner']][rslt_stats['is_trial'] == 0]
        trap_stats['track_trap_perf'] = trap_stats['track_trap'].map(trap_stats.groupby('track_trap')['is_winner'].mean())
        mapping_trap_perf = dict(trap_stats[['track_trap', 'track_trap_perf']].values)
        if ENV == 'UAT':
            trap_stats.to_csv('src/utils/assets/debug/trap_stats.csv', index=False)
        return mapping_trap_perf

    def calculate_trainer_stats(self) -> Dict:
        '''
        calculates strike rate per trainer
        :param rslt: dataframe with entire histo
        :return: trainer stats in dict
        :can be ran once a day only at start of the day
        '''
        rslt_stats = self.db_conn.get_greyhounds_full_data_trainer_stats()
        rslt_stats = rslt_stats[rslt_stats['is_trial'] == 0]
        rslt_stats['is_winner'] = np.where(rslt_stats['order'] == 1, 1, 0)
        rslt_stats['trainer_strike_rate'] = rslt_stats['trainer'].map(rslt_stats.groupby('trainer')['is_winner'].mean())
        mapping_trainer_stats = dict(rslt_stats[['trainer', 'trainer_strike_rate']].values)
        if ENV == 'UAT':
            rslt_stats[['trainer', 'trainer_strike_rate']].drop_duplicates().to_csv('src/utils/assets/debug/trainer_stats.csv')
        return mapping_trainer_stats

    def get_dog_static_data(self) -> Dict:
        '''
        return 3 dicts to map dog_name to
        dob
        trainer
        gender
        '''
        # prepare mapping of dob
        rslt = self.db_conn.get_greyhounds_full_data_dog_stats()
        dobs = rslt[rslt.DoB != ''][['dog_name', 'DoB']]
        dobs.drop_duplicates(subset=['dog_name'], inplace=True, keep='last')
        dob_dict = dict(dobs.values)

        # prepare mapping of trainers
        trainer_dict = dict(rslt[rslt.trainer != ''][['dog_name', 'trainer']].values)

        # prepare mapping of gender
        gender_dict = dict(rslt[rslt.dog_genderB0D1 < 2][['dog_name', 'dog_genderB0D1']].values)
        return {'dob': dob_dict, 'trainer': trainer_dict, 'gender': gender_dict}

    def get_current_racecard(self, market_id: str) -> pd.DataFrame:
        '''
        Gets data for the current race
        :return:
        '''
        market_desc = self.data_source.get_market_description(market_id=market_id)
        market_desc = market_desc[market_desc.status == 'ACTIVE']  # only keep active runners

        # check the track is in TRACKS
        assert len(market_desc) > 0
        assert market_desc.iloc[0]['track'] in TRACKS, f"Track {market_desc.iloc[0]['track']} not in TRACKS"

        def _get_age(dob: str, race_date: dt.datetime):
            '''
            Calculate the age of the dog with reference to the ref_date. Should be set to today in prod
            :param dog_name: string
            :param ref_date: datetime
            :param dob_map: dict
            :return: age in days
            '''
            if dob == 0 or dob == '0' or dob is None:
                return 0
            else:
                dob_dt = dt.datetime.strptime(dob, '%b-%Y')
                delta = dt.datetime(race_date.year, race_date.month, race_date.day) - dob_dt
                return round(int(delta.days) / 365, 4)
        rslt_current_race = pd.DataFrame([[str(row['market_id']),
                                           row['selection_id'],
                                           - 1,
                                           - i,
                                           row['start_time_utc'].date(),
                                           row['grade'],
                                           float(row['distance']),
                                           row['dog_name'],
                                           int(row['trap']),
                                           1,
                                           self.dog_static_data['dob'].get(row['dog_name']),
                                           30, 4, 16, 16, 30,
                                           self.dog_static_data['gender'].get(row['dog_name']),
                                           0,
                                           '2/1',
                                           _get_age(self.dog_static_data['dob'].get(row['dog_name']), row['start_time_utc']),
                                           row['grade_raw'],
                                           row['start_time_utc'],
                                           row['track'],
                                           self.dog_static_data['trainer'].get(row['dog_name']),
                                           row['is_or']
                                           ] + [int(row['track'] == track) for track in TRACKS] + [0]
                                          for i, row in market_desc.iterrows()], columns=['market_id', 'selection_id'] + BASIC_COLUMNS + ['track_' + track for track in TRACKS] + ['corr'])
        rslt_current_race['raceDate'] = pd.to_datetime(rslt_current_race['raceDate'], format='%Y-%m-%d')
        rslt_current_race['raceDateTime'] = pd.to_datetime(rslt_current_race['raceDateTime'])
        return rslt_current_race

    def get_current_racecard_with_features(self, market_id: str) -> pd.DataFrame:
        '''
        Merges today's racecard with histo. Only keep rows used for feature calculations. Calculate features and return columns for prediction
        :return:
        '''
        current_race_card = self.get_current_racecard(market_id=market_id)
        data = self.db_conn.get_greyhound_data(dog_list=current_race_card.dog_name.values.tolist())
        full_data_set = pd.concat([current_race_card, data],
                                  axis=0,
                                  ignore_index=True)
        data_speed = self.db_conn.get_speed_data(dog_list=current_race_card.dog_name.values.tolist())
        full_data_set_speed = pd.concat([current_race_card, data_speed],
                                        axis=0,
                                        ignore_index=True)
        data_with_features = self.calculate_features(full_data_set, full_data_set_speed)
        return data_with_features[data_with_features['url'] == -1]

    def correct_speed(self, df) -> pd.DataFrame:
        '''
        Speed handicapping model
        :param df:
        :return:
        '''
        raise NotImplementedError

