import sqlite3 

class RSLManager:
    def __init__(self, db_name):
        self.name = db_name
        self.database = None
        self.curr = None

    def open_connection(self):
        try:
            self.database = sqlite3.connect(self.name)
            self.curr = self.database.cursor()
            print(f"Connected to {self.name}")
        except Exception as e:
            print(f"Connection Failed: {e}")

    def create_schema(self):
        if self.database:

            # Scrap Table
            self.curr.execute(
                """
                CREATE TABLE IF NOT EXISTS Scrap (
                id INTEGER PRIMARY KEY AUTOINCREMENT ,
                date DATE,
                so_pn INTEGER(7) NOT NULL,
                component_pn INTEGER(9) NOT NULL,
                scrap_code INTEGER(3) NOT NULL,
                FOREIGN KEY(so_pn) REFERENCES ShopOrders(tl_pn),
                FOREIGN KEY(component_pn) REFERENCES Component(pn),
                FOREIGN KEY(scrap_code) REFERENCES ScrapCodes(id)
                )
                """
            )

            # Shop Order Table
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
                pn INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
                description TEXT NOT NULL,
                tl_pn INTEGER(9) NOT NULL, 
                FOREIGN KEY(tl_pn) REFERENCES VoyantModels(tl_pn)
                )
                """
            )

    def display_tables(self):
        if self.database:
            sql_query = """SELECT name FROM sqlite_master WHERE type = 'table'"""
            self.curr.execute(sql_query)
            return self.curr.fetchall()

    def load_references(self):
        pass