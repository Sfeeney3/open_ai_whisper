import mysql.connector
import pandas as pd
from sqlalchemy import create_engine


class dbObject:
    
        
    def __init__(self):
        print("Initializing dbObject")
        

        
        self.table = 
        self.user = 
        self.password = 
        self.host = 
        self.port = 
        self.database = '
            
            
        self.con = self.get_connection()
        
        try:
            self.get_connection()
            print("successful \n")
        except Exception as e:
            print("fail：case%s" % e)
    
    
    def get_connection(self):
        return create_engine(
            url="mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}".format(
                self.user, self.password, self.host, self.port, self.database
            )
        )
        
        
    def close_connection(self):
        try:
            self.con.connect().close()
            print("successful \n")
        except Exception as e:
            print("fail：case%s" % e)
                
    def selectSql(self,query):
        try:
            res = self.con.execute(query).fetchall()
            print("successful \n")
        except Exception as e:
            print("fail：case%s" % e)
    
        return res
    
    def insertSql():
        pass
    
    def insertPandasDf(self, df):
        try:
            df.to_sql(con=self.con, name=self.table, if_exists='append')
        except Exception as e:
            print("fail：case%s" % e)
            
    def selectPandasDf(self,query):
        try:
            res = pd.read_sql(query,con=self.con)
        except Exception as e:
            print("fail：case%s" % e)
        return res
            
            
            
            
            
            
            
                   
