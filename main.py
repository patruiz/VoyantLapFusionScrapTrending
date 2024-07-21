import os 
from src.database.database_manager import RSLManager

def main():
    def main_ass():
        fuck.create_schema()
        fuck.input_models(models)
        fuck.input_data(rsl)

    def test_ass():
        fuck.analyze_shoporder_scrap('1510779')
    
    
    

    
    db_name = 'LapFusionRSL.db'
    fuck = RSLManager(db_name)
    fuck.open_connection()

    rsl = os.path.join(os.getcwd(), 'rsl', 'LapFusion_RSL_2024.csv')
    models = os.path.join(os.getcwd(), 'references', 'VoyantModels.csv')
    
    
    
    main_ass()
    # test_ass()


if __name__ == '__main__':
    os.system('clear')
    main()