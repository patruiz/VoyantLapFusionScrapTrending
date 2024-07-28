import os 
from src.database.database_manager import RSLManager

def main():
    def clear_cheese():
        if os.path.exists(db_name):
            os.remove(db_name)

    def create_cheese():
        MASTER_CHEESE = RSLManager(db_name)
        MASTER_CHEESE.open_connection()
        MASTER_CHEESE.create_schema()
        MASTER_CHEESE.load_references('Plants', plants)
        MASTER_CHEESE.load_references('Codes', dm1_codes, 'DM1')
        MASTER_CHEESE.load_references('Codes', qcdm1_codes, 'QC-DM1')
        MASTER_CHEESE.load_references('Operations', dm1_operations, 'DM1')
        MASTER_CHEESE.load_references('Operations', qcdm1_operations, 'QC-DM1')
        MASTER_CHEESE.load_references('Models', models)
        # cheeseball.input_data(rsl)

    def run_cheese():
        PARMESAN = RSLManager(db_name)
        PARMESAN.open_connection()
        PARMESAN.run_rsl(rsl)
        # cheeseball.analyze_QCscrap()
        
    def scrap_cheese():
        MOZZARELLA = RSLManager(db_name)
        MOZZARELLA.open_connection()
        MOZZARELLA.main_scrap_function()
        MOZZARELLA.export_table('QCScrapLog')
        MOZZARELLA.remove_blank_columns('QCScrapLog_export.csv')
        
    def analysis_cheese():
        GOUDA = RSLManager(db_name)
        GOUDA.open_connection()
        # GOUDA.main_analysis_function(1510779)
        # ANALYZE_CHEESE.

    def test_cheese():
        # CHEESE_DANNY.get_shoporder_scrap('1510779')
        # CHEESE_DANNY.update_scraplog()
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
    scrap_cheese()
    # analysis_cheese()
    # test_cheese()

if __name__ == '__main__':
    os.system('clear')
    main()