import os
import csv
import math
import sqlite3 
import pandas as pd 

class RSLManager:
    def __init__(self, db_name):
        self.name = db_name
        self.database = None
        self.curr = None
        self.change_log = []

    # GENERAL DATABASE FUNCTIONS
    def open_connection(self):
        try:
            self.database = sqlite3.connect(self.name)
            self.curr = self.database.cursor()
            # print(f"\nConnected to {self.name}")
        except Exception as e:
            # print(f"Connection Failed: {e}")
            pass
        
    def close_connection(self):
        self.database.close()
                
    def commit_changes(self):
        if self.database:
            self.database.commit()

    # CSV FUNCTIONS
    def export_table(self, table):
        if (self.database and self.curr) != None:
            self.curr.execute(f"""SELECT * FROM {table}""")
            rows = self.curr.fetchall()
            
            csv_name = f"{table}.csv"
            with open (csv_name, "w") as csvfile:
                csv_writer = csv.writer(csvfile, delimiter = ",")
                csv_writer.writerow(i[0] for i in self.curr.description)
                for row in rows:
                    csv_writer.writerow(row)
                    
            self._remove_blank_columns(csv_name)
            self._add_device_models(csv_name)
            self._generate_model_csv(csv_name, table)
            self._delete_main_csv(csv_name)
                            
    def _remove_blank_columns(self, csvfile):
        df = pd.read_csv(csvfile)
        df = df.loc[:, (df != 0).any(axis=0)]
        df.to_csv(csvfile)
        
    def _add_device_models(self, csvfile):
        df = pd.read_csv(csvfile)
        model_list = []
        for index, val in df.iterrows():
            # print(f"\n{val.loc['shoporder']}")
            self.curr.execute("""SELECT model FROM ShopOrders INNER JOIN LapFusionModels ON LapFusionModels.tl_pn = ShopOrders.tl_pn WHERE ShopOrders.num = ?""", (int(val.loc['shoporder']), ))
            model_list.append(self.curr.fetchone()[0])
        df.insert(2, 'Model', model_list, True)
        df.to_csv(csvfile, index = False)
        
    def _generate_model_csv(self, csvfile, table):
        og_df = pd.read_csv(csvfile)
        columns = og_df.columns
        self.curr.execute("""SELECT * FROM LapFusionModels""")
        models = [i[1] for i in self.curr.fetchall()]
        models = list(set(models))
        
        for model in models:
            file_name = f"{csvfile[:-4]}_{model}.csv"
            try:
                df = og_df[og_df['Model'] == model]
            except:
                df = pd.DataFrame(columns = columns)
            
            save_path = os.path.join(os.getcwd(), 'results', table)
            file_name = f"{csvfile[:-4]}_{model}.csv"
            file_path = os.path.join(save_path, file_name)
            df.to_csv(file_path, index = False)
            
    def _delete_main_csv(self, csvfile):
        csv_path = os.path.join(os.getcwd(), csvfile)
        os.remove(csv_path)
    
        
    # SCHEMA FUNCTION
    def create_schema(self):
        if self.database:
            self._create_rsl_table()
            self._create_shoporders_table()
            self._create_lapfusionmodels_table()
            self._create_scrapcodes_table()
            self._create_components_table()
            self._create_plant_table()
            self._create_operations_table()

    def _create_rsl_table(self):
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS RSL (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            so INTEGER(7) NOT NULL,
            component_pn INTEGER(9) NOT NULL,
            scrap_code INTEGER(3) NOT NULL,
            cost REAL,
            scrap_qty INTEGER NOT NULL,
            plant TEXT NOT NULL,
            FOREIGN KEY(so) REFERENCES ShopOrders(num),
            FOREIGN KEY(component_pn) REFERENCES Component(component_pn),
            FOREIGN KEY(scrap_code) REFERENCES ScrapCodes(id),
            FOREIGN KEY(plant) REFERENCES Plant(name)
            )
            """
        )

    def _create_shoporders_table(self):
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS ShopOrders (
            num INTEGER(7) PRIMARY KEY NOT NULL,
            tl_pn INTEGER(9) NOT NULL,
            description VARCHAR,
            qty INTEGER NOT NULL,
            type TEXT,
            FOREIGN KEY(tl_pn) REFERENCES LapFusionModels(tl_pn)
            )
            """
        )

    def _create_lapfusionmodels_table(self):
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS LapFusionModels (
            tl_pn INTEGER(9) PRIMARY KEY NOT NULL,
            model VARCHAR(5) NOT NULL 
            )
            """
        )

    def _create_scrapcodes_table(self):
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS ScrapCodes (
            id INTEGER NOT NULL,
            name TEXT NOT NULL,
            plant TEXT NOT NULL, 
            PRIMARY KEY (id, plant),
            FOREIGN KEY(plant) REFERENCES Plant(name)
            ) 
            """
        )

    def _create_components_table(self):
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS Components (
            component_pn INTEGER(9) NOT NULL, 
            description TEXT NOT NULL,
            tl_pn INTEGER(9) NOT NULL,
            PRIMARY KEY (component_pn, tl_pn),
            FOREIGN KEY (tl_pn) REFERENCES LapFusionModels(tl_pn)
            )
            """
        )

    def _create_plant_table(self):
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS Plants (
            name TEXT PRIMARY KEY NOT NULL UNIQUE)
            """
        )
        
    def _create_operations_table(self):
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS Operations (
            name TEXT NOT NULL,
            plant TEXT NOT NULL,
            PRIMARY KEY (name, plant)
            FOREIGN KEY (plant) REFERENCES Plants(name)
            )
            """
        )
    
    # REFERENCE TABLE FUNCTIONS
    def load_references(self, ref_type, file_path, plant = None):
        if (ref_type == 'Plants' and plant == None):
            self._load_plants(file_path)
        elif (ref_type == 'Codes' and plant != None):
            self._load_codes(file_path, plant)
        elif (ref_type == 'Operations' and plant != None):
            self._load_operations(file_path, plant)
        elif (ref_type == 'Models' and plant == None):
            self._load_models(file_path)
        self.commit_changes()
        
    def _load_plants(self, filepath):
        df = pd.read_csv(filepath)
        for index, val in df.iterrows():
            _id = val.iloc[0]
            _name = val.iloc[1]
            _obsolete = val.iloc[2]
            if _obsolete is False:
                self.curr.execute("""INSERT INTO Plants (name) VALUES (?)""", (_name, ))
    
    def _load_codes(self, filepath, plant):
        df = pd.read_csv(filepath)
        for index, val in df.iterrows():
            _id = val.iloc[0]
            _name = val.iloc[1]
            _obsolete = val.iloc[2]
            _fileid = val.iloc[3]
            if math.isnan(_id) is False:
                try:
                    self.curr.execute("""INSERT INTO ScrapCodes (id, name, plant) VALUES (?, ?, ?)""", (_id, _name, plant))
                except Exception as e:
                    pass
    
    def _load_operations(self, filepath, plant):
        df = pd.read_csv(filepath)
        for index, val in df.iterrows():
            _id = val.iloc[0]
            _name = val.iloc[1]
            _obsolete = val.iloc[2]
            if (_obsolete is False and math.isnan(_name) is False and plant != None):
                _name = f"{str(int(_name))}".zfill(4)
                self.curr.execute("""INSERT INTO Operations (name, plant) VALUES (?, ?)""", (_name, plant))
            
    def _load_models(self, filepath):
        df = pd.read_csv(filepath)
        for index, val in df.iterrows():
            _model = val.iloc[0]
            _matnum = val.iloc[1]
            self.curr.execute("""INSERT INTO LapFusionModels (tl_pn, model) VALUES (?, ?)""", (_matnum, _model))
    
    # RSL FUNCTIONS            
    def run_rsl(self, csvfile):
        df = pd.read_csv(csvfile)
        for index, val in df.iterrows():
            _rsl_num = val.loc['RSL #']
            _date = val.loc['Date']
            _type = val.loc['Type']
            _status = val.loc['Status']
            _plant = val.loc['Plant']
            _so = val.loc['Shop/Service Order #']
            _so_qty = val.loc['Shop Order Qty']
            _so_pn = val.loc['Shop Order P/N']
            _so_desc = val.loc['Shop Order P/N Desc']
            _scrap_pn = val.loc['Scrap/Rework P/N']
            _scrap_desc = val.loc['Scrap/Rework P/N Desc']
            _scrapcode_id = val.loc['Code Id']
            _scrapcode_name = val.loc['Code']
            _scrap_cost = val.loc['Cost']
            _scrap_qty = val.loc['Scrap/Rework Qty']
            
            if (self._filter_model(_so_pn) != None):
                self._add_scrap(_date, _so, _scrap_pn, _scrapcode_id, _scrap_qty, _scrap_cost, _plant)
                self._add_shoporder(_so, _so_pn, _so_desc, _so_qty, _status)
                self._add_component(_so_pn, _scrap_pn, _scrap_desc)
            
        self.commit_changes()
            
    def _filter_model(self, material_num):
        if (self.database and self.curr) != None:
            material_num = int(material_num)
            self.curr.execute("""SELECT tl_pn FROM LapFusionModels WHERE tl_pn = ?""", (material_num, ))
            result = self.curr.fetchone()
            if result:
                return result[0]
            else:
                return None
                
    def _add_scrap(self, date, so, component_pn, scrap_code, scrap_qty, scrap_cost, plant):
        if (self.database and self.curr) != None:
            if math.isnan(scrap_qty):
                scrap_qty = 0
            self.curr.execute("""INSERT INTO RSL (date, so, component_pn, scrap_code, scrap_qty, cost, plant) VALUES (?, ?, ?, ?, ?, ?, ?)""", (date, so, component_pn, scrap_code, scrap_qty, scrap_cost, plant))
                
    def _add_shoporder(self, shoporder_num, tl_pn, description, qty, so_type):
        if (self.database and self.curr) != None:
            shoporder_num = int(shoporder_num)
            self.curr.execute("""SELECT num FROM ShopOrders WHERE num = ?""", (shoporder_num, ))
            result = self.curr.fetchone()
            if result is None:
                self.curr.execute("""INSERT INTO ShopOrders (num, tl_pn, description, qty, type) VALUES (?, ?, ?, ?, ?)""", (shoporder_num, tl_pn, description, qty, so_type))
                self.curr.execute("""SELECT num FROM ShopOrders WHERE num =?""", (shoporder_num, ))
                result = self.curr.fetchone()
                
    def _add_component(self, _so_pn, _scrap_pn, _scrap_desc):
        if (self.database and self.curr) != None:
            self.curr.execute("""SELECT component_pn, tl_pn FROM Components WHERE component_pn = ? AND tl_pn = ?""", (_scrap_pn, _so_pn, ))
            result = self.curr.fetchone()
            if result == None:
                self.curr.execute("""INSERT INTO Components (component_pn, description, tl_pn) VALUES (?, ?, ?)""", (_scrap_pn, _scrap_desc, _so_pn))
            else:
                return result[0]

    # def analyze_QCscrap(self):
    #     if (self.database and self.curr) != None:
    #         self._create_QCscraplog_table()
    #         self._find_QCscrap()

    # def _create_QCscraplog_table(self):
    #     self.curr.execute(
    #         """
    #         CREATE TABLE IF NOT EXISTS QCScrapLog (
    #         shoporder INTEGER(7) PRIMARY KEY NOT NULL,
    #         FOREIGN KEY(shoporder) REFERENCES ShopOrders(num))
    #         """
    #     )
        
    #     self.curr.execute("""SELECT id FROM ScrapCodes WHERE plant = ?""", ('QC-DM1', ))
    #     for i in self.curr.fetchall():
    #         self.curr.execute(f"""ALTER TABLE QCScrapLog ADD COLUMN '{str(i[0])}' INTEGER DEFAULT 0 NOT NULL""")
    #         self.commit_changes()
    
    # def _find_QCscrap(self):
    #     self.curr.execute("""SELECT num, tl_pn FROM ShopOrders""")
    #     shoporder_list = [i for i in self.curr.fetchall()]
    #     for shoporder in shoporder_list:
    #         print(f"\nShoporder: {shoporder[0]}")
    #         self.curr.execute(f"""SELECT shoporder FROM QCScrapLog WHERE shoporder = ?""", (shoporder[0], ))
    #         result = self.curr.fetchone()
    #         # if result != None: 
    #         #     continue

    #         self.curr.execute("""SELECT scrap_code, scrap_qty FROM RSL WHERE so = ? AND component_pn = ? AND plant = ?""", (shoporder[0], shoporder[1], 'QC-DM1'))
    #         scrap_list = self.curr.fetchall()
    #         print(f"QC-DM1 Scrap List: {scrap_list}")
    #         self.curr.execute("""INSERT INTO QCScrapLog (shoporder) VALUES (?)""", (shoporder[0], ))
    #         for scrap in scrap_list:
    #             try:
    #                 self.curr.execute(f"""UPDATE QCScrapLog SET '{str(scrap[0])}' = ? WHERE shoporder = ?""", (int(scrap[1]), shoporder[0]))
    #                 self.commit_changes()
    #             except Exception as e:
    #                 print(f"Error: {e}")
                    
    # SCRAP FUNCTIONS 
    def main_scrap_function(self):
        if (self.database and self.curr) != None:
            self._create_scraplog_tables()
            self._update_scraplog_columns()
            self._add_scraplog_shoporders()
            self._input_scraplog_data()
            
            # self._get_fulldevice_scrap(shoporder)
            
    def _create_scraplog_tables(self):
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS QCScrapLog (
            shoporder INTEGER(7) PRIMARY KEY NOT NULL,
            FOREIGN KEY (shoporder) REFERENCES ShopOrders(num)
            )
            """
        )
        
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS ProdScrapLog (
            shoporder INTEGER (7) PRIMARY KEY NOT NULL, 
            FOREIGN KEY (shoporder) REFERENCES ShopOrders(num)
            )
            """
        )
    
    def _update_scraplog_columns(self):
        self.curr.execute("""SELECT name FROM ScrapCodes""")
        scrapcodes = set([i[0] for i in self.curr.fetchall()])
        
        for i in sorted(scrapcodes):
            self.curr.execute(f"""ALTER TABLE QCScrapLog ADD COLUMN '{i}' INTEGER DEFAULT 0 NOT NULL""")
            self.curr.execute(f"""ALTER TABLE ProdScrapLog ADD COLUMN '{i}' INTEGER DEFAULT 0 NOT NULL""")
        self.commit_changes()
        
    def _add_scraplog_shoporders(self):
        self.curr.execute("""SELECT num, tl_pn FROM ShopOrders""") # Creates a list of the Shop Orders in the ShopOrder Table
        for shoporder, tl_pn in self.curr.fetchall(): # Checks each Shop Order from the list to see if they exist in the ScrapLog Table
            self.curr.execute("""SELECT shoporder FROM QCScrapLog WHERE shoporder = ?""", (shoporder, ))
            # print(f"\n******************************\n     SHOP ORDER {shoporder}\n******************************")
            if self.curr.fetchone() == None: # Adds Shop Order from the list to the ScrapLog Table
                # print(f"***** Adding {shoporder} to QCScrapLog *****")
                self.curr.execute("""INSERT INTO QCScrapLog (shoporder) VALUES (?)""", (shoporder, ))
                
            self.curr.execute("""SELECT shoporder FROM ProdScrapLog WHERE shoporder = ?""", (shoporder, ))
            if self.curr.fetchone() == None:
                self.curr.execute("""INSERT INTO ProdScrapLog (shoporder) VALUES (?)""", (shoporder, ))

        self.database.commit()
        
    def _input_scraplog_data(self):
        self.curr.execute("""SELECT num, tl_pn FROM ShopOrders""")
        for shoporder, tl_pn in self.curr.fetchall():
            self.curr.execute("""SELECT so, name, scrap_code, scrap_qty, RSL.plant FROM RSL INNER JOIN ScrapCodes ON RSL.scrap_code = ScrapCodes.id WHERE so = ? AND component_pn = ?""", (shoporder, tl_pn))
            for so, name, scrap_code, scrap_qty, plant in self.curr.fetchall(): # Gets all full device scrap from RSL per Shop Order
                if plant == 'DM1':
                    self.curr.execute(f"""UPDATE ProdScrapLog SET '{name}' = ? WHERE shoporder = ?""", (scrap_qty, shoporder))
                elif plant == 'QC-DM1':
                    self.curr.execute(f"""UPDATE QCScrapLog SET '{name}' = ? WHERE shoporder = ?""", (scrap_qty, shoporder))
        self.database.commit()
            
            
            
    # REWORK FUNCTIONS
    def main_rework_function(self):
        if (self.database and self.curr) != None:
            self._create_reworklog_tables()
            self._update_reworklog_columnns()
            self._add_reworklog_shoporders()
            self._input_reworklog_data()
        
    def _create_reworklog_tables(self):
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS ProdReworkLog(
            shoporder INTEGER(7) PRIMARY KEY NOT NULL, 
            FOREIGN KEY (shoporder) REFERENCES ShopOrders(num)
            )
            """
        )
        
    def _update_reworklog_columnns(self):
        self.curr.execute("""SELECT name FROM ScrapCodes""")
        scrapcodes = set([i[0] for i in self.curr.fetchall()])
        
        for i in sorted(scrapcodes):
            self.curr.execute(f"""ALTER TABLE ProdReworkLog ADD COLUMN '{i}' INTEGER DEFAULT 0 NOT NULL""")
        self.commit_changes()
        
    def _add_reworklog_shoporders(self):
        self.curr.execute("""SELECT num, tl_pn FROM ShopOrders""")
        for shoporder, tl_pn in self.curr.fetchall():
            self.curr.execute("""SELECT shoporder FROM ProdReworkLog WHERE shoporder = ?""", (shoporder, ))
            if self.curr.fetchone() == None:
                self.curr.execute("""INSERT INTO ProdReworkLog (shoporder) VALUES (?)""", (shoporder, ))
        self.database.commit()
        
    def _input_reworklog_data(self):
        self.curr.execute("""SELECT component_pn FROM Components WHERE description = ?""", ('HUB, 5MM FUSION', ))
        hub_pns = [i[0] for i in self.curr.fetchall()]
        
        self.curr.execute("""SELECT num, tl_pn FROM ShopOrders""")
        for shoporder, tl_pn in self.curr.fetchall():
            self.curr.execute("""SELECT so, component_pn, name, scrap_code, scrap_qty, RSL.plant FROM RSL INNER JOIN ScrapCodes on RSL.scrap_code = ScrapCodes.id WHERE so = ? AND component_pn != ?""", (shoporder, tl_pn))
            for so, component_pn, name, scrap_code, scrap_qty, plant in self.curr.fetchall():
                self.curr.execute("""SELECT description FROM Components WHERE component_pn = ? AND tl_pn = ?""", (component_pn, tl_pn))
                component_name = self.curr.fetchone()[0]
                if component_pn in hub_pns:
                    if name in ['Material Overissue', 'Material Underissue', 'Fixed Quantity', 'Defective Components']:
                        continue
                    else:
                        self.curr.execute(f"""UPDATE ProdReworkLog SET '{name}' = ? WHERE shoporder = ?""", (scrap_qty/2, so))
            self.database.commit()
            


    # CHECKING FUNCTION
    def checking_function(self, shoporder, component_pn):
        if (self.database and self.curr) != None:
            # self.curr.execute("""SELECT scrap_code, scrap_qty FROM RSL WHERE so = ? AND component_pn = ?""", (shoporder, component_pn))
            self.curr.execute("""SELECT scrap_code, scrap_qty FROM RSL WHERE so = ?""", (shoporder, ))
            for code, qty in self.curr.fetchall():
                self.curr.execute("""SELECT name FROM ScrapCodes WHERE id = ?""", (code, ))
                name = [i for i in self.curr.fetchall()]
                print(name, code, qty)


    # ANALYSIS FUNCTIONS
    def main_analysis_function(self, csvfile):
        if (self.database and self.curr) != None:
            self._generate_IMR_charts(csvfile)
        
    def _generate_IMR_charts(self, csvfile):
        df = pd.read_csv(csvfile)
        self.curr.execute("""SELECT models FROM LapFusionModels""")
        models = [i[0] for i in self.fetchall()]
        
        for model in models:
            for index, val in df.iterrows():
                pass