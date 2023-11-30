import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.sql import text, func
from sqlalchemy.orm import sessionmaker,relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Float, TIMESTAMP

Base = declarative_base()

class Case_Dim(Base):
    __tablename__ = 'dim_case'
    id = Column(Integer, primary_key=True)
    status_name = Column(String)
    status_detail = Column(String)
    status = Column(String)

class Province_Dim(Base):
    __tablename__ = 'dim_province'
    province_id = Column(Integer, primary_key=True)
    province_name = Column(String)

class District_Dim(Base):
    __tablename__ = 'dim_district'
    district_id = Column(Integer, primary_key=True)
    province_id = Column(Integer)
    district_name = Column(String)

class District_Daily(Base):
    __tablename__ = 'district_daily'
    id = Column(Integer, primary_key=True, autoincrement=True)
    district_id = Column(String)
    case_id = Column(Integer)
    date = Column(String)
    total = Column(Integer)

class Province_Daily(Base):
    __tablename__ = 'province_daily'
    id = Column(Integer, primary_key=True, autoincrement=True)
    province_id = Column(String, ForeignKey('dim_province.province_id'))
    case_id = Column(Integer)
    date = Column(String)
    total = Column(Integer)

list_case = ['closecontact_dikarantina','closecontact_discarded','closecontact_meninggal','confirmation_meninggal','confirmation_sembuh','probable_diisolasi','probable_discarded','probable_meninggal','suspect_diisolasi','suspect_discarded',]
list_date = []
list_cas_id = [1,2,3,4,5,6,7,8,9,10,11]

class Transformer():
    def __init__(self, db_mysql, db_postgre):
        self.db_mysql = db_mysql
        self.db_postgre = db_postgre

    
    def transform_dim_case(self):
        
        #Buat table untuk DIM Case
        Session = sessionmaker(bind=db_postgre)
        session = Session()

        Base.metadata.create_all(engine)

        #Insert batch data, take data frame of panda as an example
        df = pd.DataFrame({"id":[1,2,3,4,5,6,7,8,9,10,11],
                            "status_name":['close_contact','close_contact','close_contact', 'confirmation', 'confirmation', 'probable', 'probable', 'probable', 'suspect', 'suspect', 'suspect'],
                            "status_detail":['dikarantina','discarded','meninggal','meninggal', 'sembuh', 'diisolasi','discarded','meninggal','diisolasi','discarded','meninggal'],
                            'status':['closecontact_dikarantina','closecontact_discarded','closecontact_meninggal','confirmation_meninggal','confirmation_sembuh','probable_diisolasi','probable_discarded','probable_meninggal','suspect_diisolasi','suspect_discarded']})
        #The first insert method (pandas to SQL)
        #Pay attention to the if_exists parameter when using to_sql. If it is replace, it will drop the table first, then create the table, and finally insert the data
        df.to_sql('dim_case',con=engine,if_exists='append',index=False)


    def transform_dim_province(self):
        #Buat table untuk DIM Case
        Session = sessionmaker(bind=db_postgre)
        session = Session()

        Base.metadata.create_all(engine)

        #Insert batch data, take data frame of panda as an example
        df = pd.DataFrame({"province_id":['1'],
                            "province_name":['jawa_barat']})
        #The first insert method (pandas to SQL)
        #Pay attention to the if_exists parameter when using to_sql. If it is replace, it will drop the table first, then create the table, and finally insert the data
        df.to_sql('dim_province',con=engine,if_exists='append',index=False)

    def transform_dim_district(self):
        #Buat table untuk DIM Case
        Session = sessionmaker(bind=db_postgre)
        session = Session()

        Base.metadata.create_all(engine)

        #Insert batch data, take data frame of panda as an example
        df = pd.DataFrame({"district_id":['3204','3217','3216','3201','3207','3203','3209','3205','3212','3215','3208','3210','3218','3214','3213','3202','3211','3206','3273','3279','3275','3271','3277','3274','3276','3272','3278'],
                            "province_id":['32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32','32'],
                            'district_name':['Kabupaten Bandung', 'Kabupaten Bandung Barat', 'Kabupaten Bekasi', 'Kabupaten Bogor', 'Kabupaten Ciamis', 'Kabupaten Cianjur', 'Kabupaten Cirebon','Kabupaten Garut', 'Kabupaten Indramayu', 'Kabupaten Karawang', 'Kabupaten Kuningan', 'Kabupaten Majalengka','Kabupaten Pangandaran', 'Kabupaten Purwakarta', 'Kabupaten Subang','Kabupaten Sukabumi', ' Kabupaten Sumedang', 'Kabupaten Tasikmalaya', 'Kota Bandung', 'Kota Banjar', 'Kota Bekasi', 'Kota Bogor', 'Kota Cimahi', 'Kota Cirebon', 'Kota Depok', 'Kota Sukabumi', 'Kota Tasikmalaya']})
        #The first insert method (pandas to SQL)
        #Pay attention to the if_exists parameter when using to_sql. If it is replace, it will drop the table first, then create the table, and finally insert the data
        df.to_sql('dim_district',con=engine,if_exists='append',index=False)


    def transform_daily_district(self):
        conn_mysql = db_mysql.connect()
        
        df_mysql= conn_mysql.execute("SELECT * FROM covid_jabar")
        
        Session = sessionmaker(bind=db_postgre)
        session = Session()

        Base.metadata.create_all(engine)

        for row in df_mysql.fetchall():
            for case in list_case:
                new_case = District_Daily_Daily(district_id=row['kode_kab'],case_id=list_cas_id[case.index], date=row['tanggal'], total=row[case])
                #Only add, but not submit. If there is an error, you can also recall (rollback)
                session.add(new_case)
                #Commit to database
                session.commit()


    def transform_daily_province(self):
        conn_mysql = db_mysql.connect()
        
        df_mysql= conn_mysql.execute("SELECT * FROM covid_jabar")
        
        #Buat table untuk DIM Case
        Session = sessionmaker(bind=db_postgre)
        session = Session()

        Base.metadata.create_all(engine)

        for row in df_mysql.fetchall():
            for case in list_case:
                for date in list_date:
                    new_case = Province_Daily(province_id=row['kode_prov'],case_id=list_cas_id[case.index], date=date, 
                    total=session.query("SELECT SUM(" + case + ") FROM covid_jabar WHERE tanggal ==" + date ))
                    #Only add, but not submit. If there is an error, you can also recall (rollback)
                    session.add(new_student)
                    #Commit to database
                    session.commit()

