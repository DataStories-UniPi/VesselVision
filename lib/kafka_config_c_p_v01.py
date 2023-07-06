"""
cfg.py
--- SET PARAMETERS FOR KAFKA SERVER & CONSUMER & PRODUCER ---
"""
from numpy import random
import pandas as pd
import os, sys


### Folder where Kafka & ZooKeeper exist
CFG_BASEPATH = os.path.dirname(__file__)
CFG_CSV_OUTPUT_DIR = os.path.join(CFG_BASEPATH, '..', 'data')

CFG_KAFKA_FOLDER = os.path.join(CFG_BASEPATH, 'kafka_2.12-2.5.0')
CFG_KAFKA_BIN_FOLDER = os.path.join(CFG_KAFKA_FOLDER, 'bin')
CFG_KAFKA_CFG_FOLDER = os.path.join(CFG_KAFKA_FOLDER, 'config')

### Dataset Paths
CFG_READ_FILE = os.path.join(CFG_CSV_OUTPUT_DIR, 'piraeus_jan_mar_2019_8h_prep_srt_w_lens.csv')
CFG_PRODUCER_SEP = ','

CFG_LOGFILE_NAME = f'{CFG_READ_FILE}.kafka.log'


### Kafka Topic Suffix
CFG_DATASET_NAME = 'piraeus_jan_mar'
CFG_DATASET_MBB = [2562079, 4508566, 2664676, 4585429] # [MIN LON, MIN LAT, MAX LON, MAX LAT]; Piraeus


CFG_DATASET_CRS = 4326
# --------------------------------------------------------------------------------------------------------------------------------------------------------


#################################################################################
############################## PRODUCER PARAMETERS ##############################
#################################################################################

### Dataset Essential Features
# ------------------------------------------------------------------
CFG_PRODUCER_KEY = 'mmsi'                            # Piraeus Dataset
CFG_PRODUCER_TIMESTAMP_NAME = 'timestamp'            # Piraeus Dataset
CFG_CONSUMER_SPEED_NAME = 'speed'                    # Piraeus Dataset
CFG_CONSUMER_COURSE_NAME = 'course'                  # Piraeus Dataset
CFG_CONSUMER_LENGTH_NAME = 'length'                  # Piraeus Dataset
CFG_CONSUMER_COORDINATE_NAMES = ['lon', 'lat']       # Piraeus Dataset
# ------------------------------------------------------------------

### Dataset special dtypes
# ------------------------------------------------------------------
CFG_PRODUCER_DTYPE = {
    CFG_PRODUCER_KEY:int, CFG_PRODUCER_TIMESTAMP_NAME:int
}
# ------------------------------------------------------------------

### Dataset Temporal Unit
# ------------------------------------------------------------------
CFG_PRODUCER_TIMESTAMP_UNIT = 's'                    # Piraeus Dataset
# ------------------------------------------------------------------

### Kafka Output Topic(s) Suffix 
### (useful for multiple concurrent instances; can be ```None```)
# ---------------------------------------------------------------
CFG_TOPIC_SUFFIX = f'_{CFG_DATASET_NAME}'

### Topic Name which Kafka Producer writes & Kafka Consumer reads
CFG_TOPIC_NAME = 'datacsv{0}'.format(CFG_TOPIC_SUFFIX)
CFG_THROUGHPUT_SCALE = 16


#################################################################################
############################### TOPIC PARAMETERS ################################
#################################################################################

### File where Kafka Consumer writes
# -------------------------------------------------------------------------------------------------------------
CFG_WRITE_FILE = os.path.join(CFG_CSV_OUTPUT_DIR, 'MessagesKafka{0}.csv'.format(CFG_TOPIC_SUFFIX))

### Topic Name which Kafka Consumer writes the Evolving Clusters at each Temporal Instance
CFG_CRI_RESULTS_TOPIC_NAME = 'criresults{0}'.format(CFG_TOPIC_SUFFIX)

### Topic Name which Kafka Consumer writes the Encountering Vessels at each Temporal Instance
CFG_ENC_RESULTS_TOPIC_NAME = 'encounteresults{0}'.format(CFG_TOPIC_SUFFIX)

### Topic Name which Kafka Consumer writes the Moving Objects' Timeslice
CFG_ALIGNMENT_RESULTS_TOPIC_NAME = 'alignedata{0}'.format(CFG_TOPIC_SUFFIX)

### Topic Name which Kafka Consumer writes the Objects' Pool (buffer)
CFG_BUFFER_DATA_TOPIC_NAME = 'buffer{0}'.format(CFG_TOPIC_SUFFIX)

### Topic num of partitions (must be an integer)
CFG_TOPIC_PARTITIONS = 1

### Num of consumers (must be an integer)
CFG_NUM_CONSUMERS = 1

### Num of consumers equal to Num of partitions
CFG_CONSUMERS_EQUAL_TO_PARTITIONS = 'yes' if CFG_TOPIC_PARTITIONS == CFG_NUM_CONSUMERS else 'no' #'yes' or 'no'
# -------------------------------------------------------------------------------------------------------------

### Buffer/Timeslice Settings
# --------------------------------------------------------------------------------------------------------------------------------------------------------
CFG_BUFFER_COLUMN_NAMES = [
    CFG_PRODUCER_KEY, CFG_PRODUCER_TIMESTAMP_NAME, *CFG_CONSUMER_COORDINATE_NAMES, CFG_CONSUMER_SPEED_NAME, CFG_CONSUMER_COURSE_NAME, CFG_CONSUMER_LENGTH_NAME
]   # UniPi AIS Antenna Dataset
# --------------------------------------------------------------------------------------------------------------------------------------------------------

### Dataset Non-essential Features
CFG_BUFFER_OTHER_FEATURES = sorted(
    list(
        set(CFG_BUFFER_COLUMN_NAMES) - (
            # set([CFG_PRODUCER_KEY]) | set([CFG_CONSUMER_SPEED_NAME]) | set([CFG_CONSUMER_COURSE_NAME]) | set([CFG_PRODUCER_TIMESTAMP_NAME]) | set(CFG_CONSUMER_COORDINATE_NAMES)
            set([CFG_PRODUCER_KEY]) | set([CFG_PRODUCER_TIMESTAMP_NAME]) | set(CFG_CONSUMER_COORDINATE_NAMES)
        )
    )
)


#################################################################################
############################## CONSUMER PARAMETERS ##############################
#################################################################################

### Number of necessary records for temporal alignment
CFG_INIT_POINTS = 2

### Data-Point Alignment Interval (seconds)
# -------------------------------------------------------------------------------------
CFG_DESIRED_ALIGNMENT_RATE_SEC = 30     # 30 seconds = 0.5 minutes
# CFG_DESIRED_ALIGNMENT_RATE_SEC = 60     # 1 minute
# CFG_DESIRED_ALIGNMENT_RATE_SEC = 120    # 2 minutes
# CFG_DESIRED_ALIGNMENT_RATE_SEC = 180    # 3 minutes
# CFG_DESIRED_ALIGNMENT_RATE_SEC = 300    # 5 minutes
# -------------------------------------------------------------------------------------


### Maximum Speed Threshold (Vessels -- m/s)
# -------------------------------------------------------------------------------------
CFG_THRESHOLD_MIN_SPEED = 1      # 1 knot   = 1.852 km/h = 0.51  meters / second
CFG_THRESHOLD_MAX_SPEED = 50     # 50 knots = 92.6 km/h  = 25.72 meters / second
# -------------------------------------------------------------------------------------


### VESSELVISIONS PARAMETERS
# # -----------------------------------------------------------------------------------
CFG_VCRA_MODEL_PATH = './data/pickle/vcra_model.pickle'
# # -----------------------------------------------------------------------------------
CFG_ALIGNMENT_MODE = 'extra'        # {'inter', 'extra'}
# # -----------------------------------------------------------------------------------
CFG_TEMPORAL_THRESHOLD = pd.Timedelta(
    2 * CFG_DESIRED_ALIGNMENT_RATE_SEC, unit="sec"
).seconds  # at least 2 timeslices UoM (e.g., 2 x 30 sec. = 1 min.)
CFG_DISTANCE_THRESHOLD = 1852   # in meters
# # -----------------------------------------------------------------------------------

CFG_CRI_DATASET_FEATURES = [
    'own_index', 'own_mmsi', 'own_geometry', 'own_speed', 'own_course',
    'target_index', 'target_mmsi', 'target_geometry', 'target_speed', 'target_course',
    'dist_euclid', 'speed_rx', 'speed_ry', 'speed_r', 'rel_movement_direction', 'azimuth_angle_target_to_own',
    'relative_bearing_target_to_own',
    'dcpa', 'tcpa', 'U_dcpa', 'U_tcpa', 'U_dist', 'U_bearing', 'U_speed', 'ves_cri'
]  # 27 Features

### DATA OUTPUT PARAMETERS
# ------------------------------------------------------------------------
CFG_SAVE_TO_FILE  = True           # **True**: Output Data to CSV File
CFG_SAVE_TO_TOPIC = True           # **True**: Output Data to Kafka Topic
# ------------------------------------------------------------------------
