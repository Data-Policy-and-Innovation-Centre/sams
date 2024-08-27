from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from concurrent.futures import ThreadPoolExecutor

Base = declarative_base()

class Student(Base):
    __tablename__ = 'students'
    id = Column(Integer, primary_key=True)
    module = Column(String)
    year = Column(Integer)
    source_of_fund = Column(Integer)
    # Add other columns as needed

class Institute(Base):
    __tablename__ = 'institutes'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    address = Column(String)
    # Add other columns as needed

class SamsDataLoader:
    def __init__(self, db_url, downloader):
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.downloader = downloader 

    def load_student_data(self, student_data):
        for data in student_data:
            student = Student(
                module=data['module'],
                year=data['year'],
                source_of_fund=data['source_of_fund'],
                # Add other columns as needed
            )
            self.session.add(student)
        self.session.commit()

    def load_institute_data(self, institute_data):
        for data in institute_data:
            institute = Institute(
                name=data['name'],
                address=data['address'],
                # Add other columns as needed
            )
            self.session.add(institute)
        self.session.commit()

    def close(self):
        self.session.close()