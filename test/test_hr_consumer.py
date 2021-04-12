import sqlite3
import sys
import pandas as pd
sys.path.append('/app/')
from pandas._testing import assert_frame_equal

db_file_path = "./db/dota_warehouse.db"

def main():
    print("Starting the test.. You can grab a cup of coffee :D")

    if assert_frame_equal(df_result, df_accepted) is None:
        print("End to End Test were passed successfully")

if __name__ == '__main__':
    main()