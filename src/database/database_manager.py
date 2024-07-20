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

            # Scrap Table
            self.curr.execute(
                """
                CREATE TABLE IF NOT EXISTS Scrap (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                so INTEGER(7) NOT NULL,
                component_pn INTEGER(9) NOT NULL,
                scrap_code INTEGER(3) NOT NULL,
                cost REAL,
                scrap_qty INTEGER NOT NULL,
                FOREIGN KEY(so) REFERENCES ShopOrders(num),
                FOREIGN KEY(component_pn) REFERENCES Component(component_pn),
                FOREIGN KEY(scrap_code) REFERENCES ScrapCodes(id)
                )
                """
            )

            # Shop Orders Table
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

            # Voyant Models Table
            self.curr.execute(
                """
                CREATE TABLE IF NOT EXISTS VoyantModels (
                tl_pn INTEGER(9) PRIMARY KEY NOT NULL,
                model VARCHAR(5) NOT NULL 
                )
                """
            )

            # Scrap Codes Table
            self.curr.execute(
                """
                CREATE TABLE IF NOT EXISTS ScrapCodes (
                id INTEGER(3) PRIMARY KEY NOT NULL,
                name TEXT NOT NULL
                ) 
                """
            )

            # Component Table
            self.curr.execute(
                """
                CREATE TABLE IF NOT EXISTS Components (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                component_pn INTEGER NOT NULL, 
                description TEXT NOT NULL
                )
                """
            )

    def display_tables(self):
        if self.database:
            sql_query = """SELECT name FROM sqlite_master WHERE type = 'table'"""
            self.curr.execute(sql_query)
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

    def add_or_get_scrapcode(self, id, name):
        if (self.database and self.curr) != None:
            id = int(id)
            self.curr.execute("""SELECT id FROM ScrapCodes WHERE id = ?""", (id, ))
            result = self.curr.fetchone()
            if result:
                return result[0]
            else:
                self.curr.execute("""INSERT INTO ScrapCodes (id, name) VALUES (?, ?)""", (id, name))
                self.curr.execute("""SELECT id FROM ScrapCodes WHERE id = ?""", (id, ))
                result = self.curr.fetchone()

    def add_scrap(self, date, so, component_pn, scrap_code, scrap_qty, scrap_cost):
        if (self.database and self.curr) != None:
            if math.isnan(scrap_qty):
                scrap_qty = 0
            self.curr.execute("""INSERT INTO Scrap (date, so, component_pn, scrap_code, scrap_qty, cost) VALUES (?, ?, ?, ?, ?, ?)""", (date, so, component_pn, scrap_code, scrap_qty, scrap_cost))

    def add_or_get_components(self, component_pn, description):
        if (self.database and self.curr) != None:
            self.curr.execute("""INSERT INTO Components (component_pn, description) VALUES (?, ?)""", (component_pn, description))

    def input_models(self, csvfile):
        df = pd.read_csv(csvfile)
        print(f"\n{df.head()}")
        for index, val in df.iterrows():
            _mat_num = val.loc['Material Number']
            _model = val.loc['Model']
            self.add_or_get_model(_mat_num, _model)
        self.database.commit()

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
                self.add_or_get_scrapcode(_scrapcode_id, _scrapcode_name)
                self.add_scrap(_date, _so, _scrap_pn, _scrapcode_id, _scrap_qty, _scrap_cost)
                self.add_or_get_components(_scrap_pn, _scrap_desc)
            
        self.database.commit()

    def run_analysis(self, shoporder):
        shoporder = int(shoporder)
        self.curr.execute("""SELECT id FROM Scrap WHERE so = ?""", (shoporder, ))
        scrap_list = self.curr.fetchall()
        print(len(scrap_list))
        print(scrap_list)