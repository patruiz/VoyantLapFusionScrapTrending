import os 
from src.database.database_manager import RSLManager

def main():
    def clear_cheese():
        if os.path.exists(db_name):
            os.remove(db_name)

    def create_cheese():
        cheeseball = RSLManager(db_name)
        cheeseball.open_connection()
        cheeseball.create_schema()
        cheeseball.load_references('Plants', plants)
        cheeseball.load_references('Codes', dm1_codes, 'DM1')
        cheeseball.load_references('Codes', qcdm1_codes, 'QC-DM1')
        cheeseball.load_references('Operations', dm1_operations, 'DM1')
        cheeseball.load_references('Operations', qcdm1_operations, 'QC-DM1')
        cheeseball.load_references('Models', models)
        # cheeseball.input_data(rsl)

    def run_cheese():
        cheeseball = RSLManager(db_name)
        cheeseball.open_connection()
        cheeseball.run_rsl(rsl)
        # cheeseball.analyze_QCscrap()
        cheeseball.analyze_scrap(1514189)
        # cheeseball.export_table('ScrapLog')

    def test_cheese():
        # cheeseball.get_shoporder_scrap('1510779')
        # cheeseball.update_scraplog()
        pass
    
    
    
    
    db_name = 'LapFusionRSL.db'

    plants = os.path.join(os.getcwd(), 'references', 'Plants.csv')
    dm1_codes = os.path.join(os.getcwd(), 'references', 'DM1Codes.csv')
    qcdm1_codes = os.path.join(os.getcwd(), 'references', 'QC-DM1Codes.csv')
    dm1_operations = os.path.join(os.getcwd(), 'references', 'DM1Operations.csv')
    qcdm1_operations = os.path.join(os.getcwd(), 'references', 'QC-DM1Operations.csv')
    rsl = os.path.join(os.getcwd(), 'rsl', 'LapFusion_RSL_2024.csv')
    models = os.path.join(os.getcwd(), 'references', 'LapFusionModels.csv')
    
    clear_cheese()
    
    create_cheese()
    run_cheese()
    # test_cheese()

if __name__ == '__main__':
    os.system('clear')
    main()