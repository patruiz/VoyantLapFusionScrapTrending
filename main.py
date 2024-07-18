import os 
from src.database.database_manager import RSLManager

def main():
    db_name = 'LapFusionRSL.db'
    LapFusionRSL = RSLManager(db_name)
    LapFusionRSL.open_connection()
    LapFusionRSL.create_schema()
    print(LapFusionRSL.display_tables())

if __name__ == '__main__':
    os.system('clear')
    main()