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
        PARMESAN.run_rsl(rsl_2024)
        PARMESAN.run_rsl(rsl_2023)
        PARMESAN.run_rsl(rsl_2022)
        PARMESAN.run_rsl(rsl_2021)
        PARMESAN.run_rsl(rsl_2020)
        PARMESAN.run_rsl(rsl_2019)
        PARMESAN.close_connection()
        # cheeseball.analyze_QCscrap()
        
    def scrap_cheese():
        MOZZARELLA = RSLManager(db_name)
        MOZZARELLA.open_connection()
        MOZZARELLA.main_scrap_function()
        MOZZARELLA.close_connection()
        
    def rework_cheese():
        CHEDDAR = RSLManager(db_name)
        CHEDDAR.open_connection()
        CHEDDAR.main_rework_function()
        CHEDDAR.close_connection()
        
    def export_cheese():
        GOUDA = RSLManager(db_name)
        GOUDA.open_connection()
        GOUDA.export_table('QCScrapLog')
        GOUDA.export_table('ProdScrapLog')
        GOUDA.export_table('ProdReworkLog')
        GOUDA.close_connection()
        
    def update_cheese():
        ITSWEDNESDAYMYDUDES = RSLManager(db_name)
        ITSWEDNESDAYMYDUDES.open_connection()
        ITSWEDNESDAYMYDUDES.main_update_function()
        ITSWEDNESDAYMYDUDES.close_connection()
        
    def analyze_cheese():
        REEEEEEEEE = RSLManager(db_name)
        REEEEEEEEE.open_connection()
        REEEEEEEEE._generate_yield_chart('EB215')
        REEEEEEEEE.close_connection()

    db_name = 'LapFusionRSL.db'

    plants = os.path.join(os.getcwd(), 'references', 'Plants.csv')
    dm1_codes = os.path.join(os.getcwd(), 'references', 'DM1Codes.csv')
    qcdm1_codes = os.path.join(os.getcwd(), 'references', 'QC-DM1Codes.csv')
    dm1_operations = os.path.join(os.getcwd(), 'references', 'DM1Operations.csv')
    qcdm1_operations = os.path.join(os.getcwd(), 'references', 'QC-DM1Operations.csv')
    rsl_2024 = os.path.join(os.getcwd(), 'rsl', 'RSL_2024.csv')
    rsl_2023 = os.path.join(os.getcwd(), 'rsl', 'RSL_2023.csv')
    rsl_2022 = os.path.join(os.getcwd(), 'rsl', 'RSL_2022.csv')
    rsl_2021 = os.path.join(os.getcwd(), 'rsl', 'RSL_2021.csv')
    rsl_2020 = os.path.join(os.getcwd(), 'rsl', 'RSL_2020.csv')
    rsl_2019 = os.path.join(os.getcwd(), 'rsl', 'RSL_2019.csv')
    models = os.path.join(os.getcwd(), 'references', 'LapFusionModels.csv')
    
    # clear_cheese()
    # create_cheese()
    # run_cheese()
    # scrap_cheese()
    # rework_cheese()
    # export_cheese()
    # update_cheese()
    analyze_cheese()
    
    # check_cheese()
    


if __name__ == '__main__':
    os.system('clear')
    main()
