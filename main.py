import os 
from src.database.database_manager import RSLManager

def main():
    def clear_ass():
        if os.path.exists(db_name):
            os.remove(db_name)

    def create_ass():
        cheeseball.create_schema()
        cheeseball.input_models(models)
        cheeseball.input_data(rsl)

    def test_ass():
        cheeseball.get_shoporder_scrap('1510779')
        # cheeseball.update_scraplog()
    
    db_name = 'LapFusionRSL.db'

    clear_ass()

    cheeseball = RSLManager(db_name)
    cheeseball.open_connection()

    rsl = os.path.join(os.getcwd(), 'rsl', 'LapFusion_RSL_2024.csv')
    models = os.path.join(os.getcwd(), 'references', 'VoyantModels.csv')

    create_ass()

    # test_ass()


if __name__ == '__main__':
    os.system('cls')
    main()