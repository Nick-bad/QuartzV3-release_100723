import datetime as dt


TRACKS = ['Belle Vue', 'Central Park', 'Coventry', 'Crayford', 'Doncaster', 'Hall Green', 'Harlow', 'Henlow', 'Hove',
          'Kinsley', 'Monmore', 'Newcastle', 'Nottingham', 'Pelaw Grange', 'Perry Barr', 'Peterborough', 'Poole',
          'Romford', 'Shawfield', 'Sheffield', 'Sunderland', 'Swindon', 'Towcester', 'Wimbledon', 'Yarmouth']


RACES = {
    '1.000': {
        'track': 'Towcester',
        'distance': 500,
        'grade': 2,
        'race_dt': dt.datetime(2023, 6, 24, 17, 20, 0),
        'runners': [
            [2, 'Alineas Debut'],
            [6, 'Swithins Teejay'],
            [5, 'Rapido Lady'],
            [4, 'Arthur Cucumber'],
            [3, 'Savana Top Cat'],
            [1, 'Ballymac John']
        ],
        'corr': 0,
        'is_or': 1
    },

    '1.001': {
        'track': 'Harlow',
        'distance': 415,
        'grade': 7,
        'race_dt': dt.datetime(2023, 5, 7, 21, 28, 0),
        'runners': [
            [6, 'Ballynew Belle'],
            [1, 'Swift Kettle'],
            [5, 'Jura Kazura'],
            [4, 'Milltown Rose'],
            [3, 'Jura Go Loupi'],
            [2, 'Karlow Kat']
        ],
        'corr': -20,
        'is_or': 0
    },

    '1.002': {
        'track': 'Henlow',
        'distance': 460,
        'grade': 1,
        'race_dt': dt.datetime(2023, 5, 7, 21, 18, 0),
        'runners': [
            [6, 'Slippin Jimmy'],
            [5, 'Caseys Tommy'],
            [1, 'Liffeyside Blake'],
            [2, 'Jogon Blue'],
            [4, 'Flitwick Club']
        ],
        'corr': 0,
        'is_or': 0
    },

    '1.003': {
        'track': 'Kinsley',
        'distance': 462,
        'grade': 4,
        'race_dt': dt.datetime(2023, 5, 7, 21, 16, 0),
        'runners': [
            [6, 'Curryhills Tiana'],
            [3, 'Meenagh Madman'],
            [5, 'Vociferous'],
            [1, 'Westfield Jinn'],
            [2, 'Ballymac Echo'],
            [4, 'Eternal']
        ],
        'corr': 20,
        'is_or': 0
    },
}

EXP_QUARTZ = {
    "Stealthescene": 0.564039085166669,
    "Ballynabee Candy": 0.529836443978662,
    "Moaning Amy": 0.380190111958121,
    "Beautiful Blaze": 0.47517711467889,
    "Headleys Kane": 0.285623665754148,
    "The Other Sunset": 0.573349133558125,
    "Hazelhill Dream": 0.485328468528179,
    "Toms Cloda": 0.51555653669807,
    "Chucker": 0.462085526571935,
    "A Bit Of Respect": 0.454949255530034,
    "Adrigole Hawk": 0.528888725508436,
    "Lockdown Kate": 0.396234563631961,
    "Bubbly Bonanza": 0.646409518002629,
    "Space Traveller": 0.466565903550662,
    "To Tone Grace": 0.217031777790358,
    "Lambstown Breeze": 0.619371647532324,
    "Ballyard Hilary": 0.506800647933541,
    "Droopys Toast": 0.381562216022036,
    "Wychwood Katie": 0.547287511400879,
    "Mullocks Miya": 0.393330254385324,
    "Westlake Finn": 0.273182391651196,
    "Fireball Lee": 0.56308395018902,
    "Lylas Smiler": 0.446537445547542,
    "Icaals Rocco": 0.599591599751861,
    "Minesapinacolada": 0.518782203693626,
    "Springtown Hugo": 0.400531803189373,
    "Brosna Sally": 0.367686993357573,
    "Turbine Trooper": 0.601247583177256,
    "Shebas Eske": 0.28991942737306,
    "Tweedledum": 0.517540330533208,
    "Crossfield Tara": 0.556149288373062,
    "Roeshill Daisy": 0.440999712481573,
    "Ha Ha Dnata": 0.537589657939716,
    "Too Many Men": 0.419079794830466,
    "Nissels Blade": 0.425665805511303,
    "Minnies Vnukovo": 0.534449583188089
}