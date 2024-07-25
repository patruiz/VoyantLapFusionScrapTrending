import os
import csv
import math
import string
import sqlite3 
import pandas as pd 

class RSLManager:
    def __init__(self, db_name):
        self.name = db_name
        self.database = None
        self.curr = None
        self.change_log = []

    def open_connection(self):
        try:
            self.database = sqlite3.connect(self.name)
            self.curr = self.database.cursor()
            # print(f"\nConnected to {self.name}")
        except Exception as e:
            # print(f"Connection Failed: {e}")
            pass
                
    def commit_changes(self):
        if self.database:
            self.database.commit()

    def export_table(self, table):
        if (self.database and self.curr) != None:
            self.curr.execute(f"""SELECT * FROM {table}""")
            rows = self.curr.fetchall()

            with open (f"{table}_export.csv", "w") as csvfile:
                csv_writer = csv.writer(csvfile, delimiter = "\t")
                csv_writer.writerow(i[0] for i in self.curr.description)
                for row in rows:
                    csv_writer.writerow(row)

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
                    
    def analyze_scrap(self, shoporder):
        if (self.database and self.curr) != None:
            self._create_scraplog_table()
            self._enter_scraplog_data()
            # self._get_fulldevice_scrap(shoporder)
            
    def _create_scraplog_table(self):
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS ScrapLog (
            shoporder INTEGER(7) PRIMARY KEY NOT NULL,
            FOREIGN KEY(shoporder) REFERENCES ShopOrders(num))
            """
        )
        
        self.curr.execute("""SELECT name FROM ScrapCodes""")
        scrapcodes = set([i[0] for i in self.curr.fetchall()])
        
        for i in sorted(scrapcodes):
            self.curr.execute(f"""ALTER TABLE ScrapLog ADD COLUMN '{i}' INTEGER DEFAULT 0 NOT NULL""")
        self.commit_changes()
    
    def _enter_scraplog_data(self):
        self.curr.execute("""SELECT num, tl_pn FROM ShopOrders""") # Creates a list of the Shop Orders in the ShopOrder Table
        
        for shoporder, tl_pn in self.curr.fetchall(): # Checks each Shop Order from the list to see if they exist in the ScrapLog Table
            self.curr.execute("""SELECT shoporder FROM ScrapLog WHERE shoporder = ?""", (shoporder, ))
            if self.curr.fetchone(): # Checks if the Shop Order exists within the ScrapLog Table
                # Need to create a list of all scrapcodes available or whatever 
                self.curr.execute(f"""SELECT {scrap_name} FROM ScrapLog WHERE shoporder = ?""", (scrapname?????, shoporder)) # Checks if a Scrap Name exists within a Shop Order inside the ScrapLog Table
                if self.curr.fetchone():
                    print('nice nerd, now just find the qty and then add to it')
                else:
                    print("Enter in the scrap name and scrap qty into the shop order since it doesnt exist")
            else:
                print('false')



            # for scraplog_shoporder in self.curr.fetchall(): # Creates a list of all Shop Orders in the ScrapLog Table
                # print(scraplog_shoporder)
                # if scraplog_shoporder is None: # Checks each Shop Order in the ScrapLog Table
                #     print(scraplog_shoporder)
                #     # Insert scraplog_shoporder_scrapname_scrapqty
                # elif scraplog_shoporder is not None:
                #     self.curr.execute()
                #     print("find if scrap name exists in shop order")
                #     if scraplog_shoporder_scrap 


            # if check != None: # Need to check if this part of the function works while there is already existing item sin the ScrapLog
            #     self.curr.execute("""SELECT so, name, scrap_code, scrap_qty FROM RSL INNER JOIN ScrapCodes ON RSL.scrap_code = ScrapCodes.id WHERE so = ? AND component_pn = ?""", (shoporder, tl_pn))
            #     scrap_list = [i for i in self.curr.fetchall()]

            #     for so_num, scrap_name, scrap_code, scrap_qty in scrap_list:
            #         self.curr.execute(f"""SELECT '{str(scrap_qty)}' FROM ScrapLog WHERE shoporder = ? AND '{str(scrap_name)}' = ?""", (int(so_num), scrap_name))
            #         swag = self.curr.fetchall()
            # else:
            #     self.curr.execute("""INSERT INTO ScrapLog (shoporder) VALUES (?)""", (shoporder, ))
            #     self.curr.execute("""SELECT so, name, scrap_code, scrap_qty FROM RSL INNER JOIN ScrapCodes ON RSL.scrap_code = ScrapCodes.id WHERE so = ? AND component_pn = ?""", (shoporder, tl_pn))
            #     scrap_list = [i for i in self.curr.fetchall()]

            #     for so_num, scrap_name, scrap_code, scrap_qty in scrap_list:
            #         self.curr.execute(f"""SELECT '{str(scrap_qty)}' FROM ScrapLog WHERE shoporder = ? AND '{str(scrap_name)}' = ?""", (int(so_num), scrap_name))
                #     swag = self.curr.fetchall()


                #     self.curr.execute(f"""UPDATE ScrapLog SET '{str(scrap[1])}' = ? WHERE shoporder = ?""", (int(scrap[3]), int(scrap[0])))
                #     self.commit_changes()
            
    def _get_fulldevice_scrap(self, shoporder):
        # self.curr.execute("""SELECT sql FROM sqlite_master WHERE tbl_name = ? AND type = 'table""", ('ScrapLog', ))
        self.curr.execute("""PRAGMA table_info(ScrapLog)""")
        scrap_codes = [i[1] for i in self.curr.fetchall()]
        
        scrap_code_names = []
        for code in scrap_codes:
            self.curr.execute("""SELECT name FROM ScrapCodes WHERE id = ?""", (code, ))
            result = self.curr.fetchone()
            if result is None:
                pass
            else:
                scrap_code_names.append((code, result[0]))
                
        df = pd.read_csv(os.path.join(os.getcwd(), 'ScrapLog_export.csv'), delimiter = "\t")
        new_headers = []
        for column in df.columns:
            for scrap in scrap_code_names:
                print(scrap)
                if str(column) == str(scrap[0]):
                    print(column, str(scrap[0]), str(scrap[1]))
                    new_headers.append(scrap[1])
        
        print(new_headers)
        df.columns = new_headers
        # print(df)      
        
        
        # print(self.curr.fetchall())   
        # self.curr.execute("""SELECT * FROM ScrapLog WHERE shoporder = ?""", (shoporder, ))
        
            
            
                
            
            
            
            
        # for shoporder, tl_pn in self.curr.fetchall():
        #     shoporders.append(shoporder), tl_pns.append(tl_pn)
        
        # for tl_pn in tl_pns:
        #     self.curr.execute("""SELECT so, scrap_code, scrap_qty FROM RSL WHERE )

        # # for shoporder in shoporders:
        # #     print(f"\nShop Order: {shoporder}")
        # #     self.curr.execute(f"""SELECT shoporder FROM ScrapLog WHERE shoporder = ? AND component)
















    def get_shoporder_scrap(self, shoporder):
        shoporder = int(shoporder)
        self.curr.execute("""SELECT tl_pn FROM ShopOrders WHERE num = ?""", (shoporder, ))
        tl_pn = self.curr.fetchone()[0]
        self.curr.execute("""SELECT scrap_code, scrap_qty, plant FROM RSL WHERE so = ? AND component_pn = ?""", (shoporder, tl_pn))
        scrap_list = self.curr.fetchall()
        
        dm1_sum, qc_sum = 0, 0
        for scrap in scrap_list:
            if scrap[2] == 'DM1':
                dm1_sum = dm1_sum + int(scrap[1])
            elif scrap[2] == 'QC-DM1':
                qc_sum = qc_sum + int(scrap[1])
        print(dm1_sum, qc_sum)


    def anal(self):
        pass
    
        