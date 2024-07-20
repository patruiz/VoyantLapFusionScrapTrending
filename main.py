import os 
from src.database.database_manager import RSLManager

def main():
    def main_ass():
        fuck.create_schema()
        fuck.input_models(models)
        fuck.input_data(rsl)

    def test_ass():
        fuck.run_analysis('1522861')
    
    db_name = 'LapFusionRSL.db'
    fuck = RSLManager(db_name)
    fuck.open_connection()

    rsl = os.path.join(os.getcwd(), 'rsl', 'LapFusion_RSL_2024.csv')
    models = os.path.join(os.getcwd(), 'references', 'VoyantModels.csv')
    # main_ass()
    test_ass()


    

def test(fuck):
    fuck.run_analysis('1522861')

if __name__ == '__main__':
    os.system('cls')
    main()