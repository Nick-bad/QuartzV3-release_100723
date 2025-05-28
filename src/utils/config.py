# ENV
ENV = 'PROD'

# Keys
MAILGUN_KEY = '### KEY ###'
BETFAIR_PASSWORD = '### PSSWD ###'
BETFAIR_APP_KEY = '### KEY ###'
DROPBOX_TOKEN = '### TOK ###'
GREYHOUND_DB_PASSWD = '### PASSWD ###'


# Paths
WINDOWS_GBGB_DATA_PATH = 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/GBGB_data_dev_V2.txt'
LINUX_GBGB_DATA_PATH = '/home/nicolas/Documents/QuartzV3/src/utils/assets/my_sql/GBGB_data_dev_V2.txt'
GREYHOUND_DATA_PKL = 'src/utils/assets/pkl/greyhound_data.pkl'
HISTO_DB = 'src/utils/assets/pkl/greyhound_data_2014_2022.pkl'
SPEED_CORR_FILE_1 = 'src/utils/assets/models/speed_corr_1.csv'
SPEED_CORR_FILE_2 = 'src/utils/assets/models/speed_corr_2.csv'
GREYHOUND_LGBM = 'src/utils/assets/models/lgbm_py38_may22.txt'
GREYHOUND_LGBM_1 = 'src/utils/assets/models/lgbm_py38_jan23.txt'
LGBM_SPEED_MODEL = 'src/utils/assets/models/lgbm_speed_model_jan23.txt'
LGBM_SECTIME_MODEL = 'src/utils/assets/models/lgbm_secTime_model_jan23.txt'
LINEAR_CORR = 'src/utils/assets/models/speed_linear_corr_model_jan23.txt'
GREYHOUND_LGBM_FEATURES = 'src/utils/assets/models/model_features_may22.pkl'
GREYHOUND_LGBM_FEATURES_1 = 'src/utils/assets/models/model_features_jan23.pkl'
GREYHOUND_LGBM_SECTIME_FEATURES = 'src/utils/assets/models/lgbm_secTime_model_features_jan23.pkl'
GREYHOUND_LGBM_SPEED_FEATURES = 'src/utils/assets/models/lgbm_speed_model_features_jan23.pkl'
LIN_REG_VARS = 'src/utils/assets/models/speed_model_linear_corr_features_jan23.pkl'
DROPBOX_QUARTZ = '/{}/current_score_cards/premium/{}'
DROPBOX_QUARTZ_ARCHIVE = '/{}/archived_score_cards/premium/{}'
TRACK_COEFFS = 'src/utils/assets/models/track_coeffs.pkl'

# Error message
FEATURE_MISSING_MESSAGE = 'Feature missing'

# Exceptions that don't receive emails
NO_EMAIL_EXCEPTIONS = [FEATURE_MISSING_MESSAGE]

TRACKS = ['Belle Vue', 'Central Park', 'Coventry', 'Crayford', 'Doncaster', 'Hall Green', 'Harlow', 'Henlow', 'Hove',
          'Kinsley', 'Monmore', 'Newcastle', 'Nottingham', 'Pelaw Grange', 'Perry Barr', 'Peterborough', 'Poole',
          'Romford', 'Shawfield', 'Sheffield', 'Sunderland', 'Swindon', 'Towcester', 'Wimbledon', 'Yarmouth']

BASIC_COLUMNS = ['url', 'uniqueKey', 'raceDate', 'grade', 'distance', 'dog_name', 'trap', 'order', 'DoB',
                 'totalTime_readable', 'secTime', 'speed', 'speed_raw', 'weight', 'dog_genderB0D1',
                 'is_trial', 'SP', 'age', 'grade_raw', 'raceDateTime', 'track', 'trainer', 'is_or']

ALLOWED_GRADES = [f'{letter}{num}' for letter in ['A', 'B', 'D', 'M', 'P', 'S'] for num in range(16)]
ALLOWED_GRADES.extend(['OR', 'OR1', 'OR2', 'OR3'])

# Column types to be preserved in dropbox
COLUMNS_TO_PUBLISH = ['trap', 'dog_name', 'track', 'grade_raw', 'quartz_pred', 'selection_id', 'market_id',
                      'raceDateTime', 'speed_pred', 'weight_avg']

SECONDS_LAG = 25 * 60

# delay prices are recorded from
RECORD_DELAY = 60 * 25