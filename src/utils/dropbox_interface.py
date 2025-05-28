import dropbox
import logging
from src.utils.config import *
import datetime as dt
from src.utils.utils import exception_handler
import pandas as pd
import csv
from tenacity import retry, wait_fixed, stop_after_attempt

logger = logging.getLogger('main_logger')


class DropBoxStore:
    def __init__(self, env: str):
        self.access_token = DROPBOX_TOKEN
        self.env = env
        self.dbx = dropbox.Dropbox(self.access_token)

    @exception_handler
    @retry(wait=wait_fixed(5), stop=stop_after_attempt(12))
    def publish_quartz(self,  df) -> None:
        '''
        If current scorecard is from yesterday => archive and publish new one. Otherwise publish
        :return: None
        '''
        # check if file is already in folder
        files = self.dbx.files_list_folder(path=DROPBOX_QUARTZ.format(self.env, ''))
        files = [f.name for f in files.entries if f.name == 'score_card.csv']
        if len(files) == 0:  # case score_card.csv not in folder
            self.dbx.files_upload(bytes(df.to_csv(index=False, header=True), 'utf8'),
                                  DROPBOX_QUARTZ.format(self.env, 'score_card.csv'),
                                  mode=dropbox.files.WriteMode.overwrite)
        else:  # case file is here
            # get metadata. If nothing found overwrite the current file
            metadata, f = self.dbx.files_download(DROPBOX_QUARTZ.format(self.env, 'score_card.csv'))
            # check if the scorecard should be archived and upload a new one
            if metadata.client_modified.date() < dt.datetime.now().date():
                self.dbx.files_move_v2(from_path=DROPBOX_QUARTZ.format(self.env, 'score_card.csv'),
                                       to_path=DROPBOX_QUARTZ_ARCHIVE.format(self.env, 'score_card_{}.csv'.format(
                                           metadata.client_modified.date())),
                                       autorename=True,
                                       allow_ownership_transfer=True)
                self.dbx.files_upload(bytes(df.to_csv(index=False, header=True), 'utf8'),
                                      DROPBOX_QUARTZ.format(self.env, 'score_card.csv'),
                                      mode=dropbox.files.WriteMode.overwrite)
            else:
                # append to existing
                new_df = pd.concat([df, self.to_pandas(f)])
                self.dbx.files_upload(bytes(self.fix_data_types(new_df).to_csv(index=False, header=True), 'utf8'),
                                      DROPBOX_QUARTZ.format(self.env, 'score_card.csv'),
                                      mode=dropbox.files.WriteMode.overwrite)

    def to_pandas(self, f):
        '''
        Loads score_card to pandas and adapts data type of select columns
        '''
        csv_reader = csv.reader(f.content.decode().splitlines(), delimiter=',')
        data = []
        for row in csv_reader:
            data.append(row)
        columns = data.pop(0)
        return pd.DataFrame(data, columns=columns)

    def fix_data_types(self, df):
        '''
        Ensures data types published in correct format. market_id already a string. selection_id must be string to avoid ".0" issues
        :param df:
        :return: df
        '''
        df['selection_id'] = df['selection_id'].astype(float).astype(int).astype(str)
        return df

if __name__ == '__main__':
    ''' just for on the fly testing '''
    dbs = DropBoxStore(env='PROD')
    metadata, f = dbs.dbx.files_download(DROPBOX_QUARTZ.format(dbs.env, 'score_card.csv'))
    print('ok')