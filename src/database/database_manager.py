import math
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
            print(f"\nConnected to {self.name}")
        except Exception as e:
            print(f"Connection Failed: {e}")

    def create_schema(self):
        if self.database:
            self._create_rsl_table()
            self._create_shoporders_table()
            self._create_voyantmodels_table()
            self._create_scrapcodes_table()
            self._create_components_table()
            self._create_QCscraplog_table()
            self._create_plant_table()

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
            FOREIGN KEY(tl_pn) REFERENCES VoyantModels(tl_pn)
            )
            """
        )

    def _create_voyantmodels_table(self):
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS VoyantModels (
            tl_pn INTEGER(9) PRIMARY KEY NOT NULL,
            model VARCHAR(5) NOT NULL 
            )
            """
        )

    def _create_scrapcodes_table(self):
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS ScrapCodes (
            id INTEGER(3) NOT NULL,
            name TEXT NOT NULL,
            plant TEXT NOT NULL, 
            PRIMARY KEY (id, plant),
            FOREIGN KEY(plant) REFERENCES Plant(name)
            ) 
            """
        )

    def _create_components_table(self):
        # Component Table YOU CAN HAVE TWO PRIMARY KEYS AND THE UNIQUE VALUE IS THE COMBINATION 
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS Components (
            component_pn INTEGER PRIMARY KEY NOT NULL, 
            description TEXT NOT NULL
            )
            """
        )

    def _create_QCscraplog_table(self):
        # QCScrapLog Table
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS QCScrapLog (
            shoporder INTEGER(7) PRIMARY KEY NOT NULL,
            FOREIGN KEY(shoporder) REFERENCES ShopOrders(num))
            """
        )

    def _create_plant_table(self):
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS Plant (
            name TEXT PRIMARY KEY NOT NULL UNIQUE)
            """
        )

    def commit_changes(self):
        if self.database:
            self.database.commit()

    def display_tables(self):
        if self.database:
            self.curr.execute("""SELECT name FROM sqlite_master WHERE type = 'table'""")
            return self.curr.fetchall()
            
    def add_or_get_shoporder(self, shoporder_num, tl_pn, description, qty, so_type):
        if (self.database and self.curr) != None:
            shoporder_num = int(shoporder_num)
            self.curr.execute("""SELECT num FROM ShopOrders WHERE num = ?""", (shoporder_num, ))
            result = self.curr.fetchone()
            if result:
                return result[0]
            else:
                self.curr.execute("""INSERT INTO ShopOrders (num, tl_pn, description, qty, type) VALUES (?, ?, ?, ?, ?)""", (shoporder_num, tl_pn, description, qty, so_type))
                self.curr.execute("""SELECT num FROM ShopOrders WHERE num =?""", (shoporder_num, ))
                result = self.curr.fetchone()
                return result[0]
            
    def get_model(self, material_num):
        if (self.database and self.curr) != None:
            material_num = int(material_num)
            self.curr.execute("""SELECT tl_pn FROM VoyantModels WHERE tl_pn = ?""", (material_num, ))
            result = self.curr.fetchone()
            if result:
                return result[0]
            else:
                return None

    def add_or_get_model(self, material_num, model):
        if (self.database and self.curr) != None:
            material_num = int(material_num)
            self.curr.execute("""SELECT tl_pn FROM VoyantModels WHERE tl_pn = ?""", (material_num, ))
            result = self.curr.fetchone()
            if result:
                return result[0]
            else:
                self.curr.execute("""INSERT INTO VoyantModels (tl_pn, model) VALUES (?, ?)""", (material_num, model))
                self.curr.execute("""SELECT tl_pn FROM VoyantModels WHERE tl_pn = ?""", (material_num, ))
                result = self.curr.fetchone()
                return result[0]

    def add_or_get_scrapcode(self, id, plant, name):
        if (self.database and self.curr) != None:
            id = int(id)
            self.curr.execute("""SELECT id, plant FROM ScrapCodes WHERE id = ? and plant = ?""", (id, plant))
            result = self.curr.fetchone()
            if result:
                return result[0]
            else:
                self.curr.execute("""INSERT INTO ScrapCodes (id, plant, name) VALUES (?, ?, ?)""", (id, plant, name))
                self.curr.execute("""SELECT id FROM ScrapCodes WHERE id = ?""", (id, ))
                result = self.curr.fetchone()
                return result

    def add_scrap(self, date, so, component_pn, scrap_code, scrap_qty, scrap_cost, plant):
        if (self.database and self.curr) != None:
            if math.isnan(scrap_qty):
                scrap_qty = 0
            self.curr.execute("""INSERT INTO RSL (date, so, component_pn, scrap_code, scrap_qty, cost, plant) VALUES (?, ?, ?, ?, ?, ?, ?)""", (date, so, component_pn, scrap_code, scrap_qty, scrap_cost, plant))

    def add_or_get_components(self, component_pn, description):
        if (self.database and self.curr) != None:
            component_pn = int(component_pn)
            self.curr.execute("""SELECT component_pn FROM Components WHERE component_pn = ?""", (component_pn, ))
            result = self.curr.fetchone()
            if result:
                return result[0]
            else:
                self.curr.execute("""INSERT INTO Components (component_pn, description) VALUES (?, ?)""", (component_pn, description))
                self.curr.execute("""SELECT component_pn FROM Components WHERE component_pn = ?""", (component_pn, ))
                result = self.curr.fetchone()
            
    def input_models(self, csvfile):
        df = pd.read_csv(csvfile)
        print(f"\n{df.head()}")
        for index, val in df.iterrows():
            _mat_num = val.loc['Material Number']
            _model = val.loc['Model']
            self.add_or_get_model(_mat_num, _model)
        self.commit_changes()

    def input_data(self, csvfile):
        df = pd.read_csv(csvfile)
        print(f"\n{df.head()}")
        for index, val in df.iterrows():
            _rsl = val.loc['RSL #']
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

            if (self.get_model(_so_pn) != None):
                self.add_or_get_shoporder(_so, _so_pn, _so_desc, _so_qty, _status)
                self.add_or_get_scrapcode(_scrapcode_id, _plant, _scrapcode_name)
                self.add_scrap(_date, _so, _scrap_pn, _scrapcode_id, _scrap_qty, _scrap_cost, _plant)
                self.add_or_get_components(_scrap_pn, _scrap_desc)
            
        self.commit_changes()

    # def get_shoporder_scrap(self, shoporder):
    #     df = pd.DataFrame(columns = ['Item', 'Qty', 'Scrap Code'])
    #     shoporder = int(shoporder)
    #     self.curr.execute("""SELECT component_pn, scrap_code, scrap_qty FROM RSL WHERE so = ?""", (shoporder, ))
    #     scrap_list = self.curr.fetchall()
        
    #     self.curr.execute("""SELECT name FROM ScrapCodes""")
    #     scrapcode_list = self.curr.fetchall()
        
    #     for i in scrap_list:
    #         self.curr.execute("""SELECT description FROM Components WHERE component_pn = ?""", (i[0], ))
    #         component_desc = self.curr.fetchone()[0]
    #         self.curr.execute("""SELECT name FROM ScrapCodes WHERE id = ?""", (i[1], ))
    #         scrapcode_name = self.curr.fetchone()[0]
    #         df.loc[-1] = [component_desc, i[2], scrapcode_name]
    #         df.index = df.index + 1
    #         df = df.sort_index()
    #     print(df)

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

    def update_QCscraplog_codes(self):
        if (self.database and self.curr) != None:
            self.curr.execute("""SELECT id FROM ScrapCodes""")
            scrapcodes = [i[0] for i in self.curr.fetchall()]

            self.curr.execute("""PRAGMA table_info(QCScrapLog)""")
            scraplog_columns = [info[1] for info in self.curr.fetchall()]

            missing_columns = [code for code in scrapcodes if str(code) not in scraplog_columns]

            for code in missing_columns:
                try:
                    self.curr.execute(f"""ALTER TABLE QCScrapLog ADD COLUMN '{str(code)}' INTEGER DEFAULT 0 NOT NULL""")
                except Exception as e:
                    print(f'Error adding Column: {e}')

            self.commit_changes()

    # def analyze_scraplog(self):
    #     if (self.database and self.curr) != None:
    #         self.curr.execute(""" """)


    def anal(self):
        pass
    
        