from sqlalchemy import create_engine, Column, Integer, String, JSON 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

Base = declarative_base()

class Student(Base):
    __tablename__ = 'students'
    barcode = Column(String, primary_key=True)
    year = Column(Integer)
    course_type = Column(String)
    type = Column(String)
    # Add other columns as needed

class Institute(Base):
    __tablename__ = 'institutes'
    sams_code = Column(Integer, primary_key=True)
    year = Column(Integer, primary_key=True)
    module = Column(String)
    name = Column(String)
    type = Column(String)
    # Add other columns as needed

class SamsDataLoader:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.executor = ThreadPoolExecutor(max_workers=10)

    def load_student_data(self, student_data):
        futures = []
        with tqdm(total=len(student_data), desc="Loading student data") as pbar:
            for data in student_data:
                future = self.executor.submit(self._add_student, data)
                futures.append(future)

            for future in as_completed(futures):
                future.result()
                pbar.update(1)

    def load_institute_data(self, institute_data):
        futures = []
        with tqdm(total=len(institute_data), desc="Loading institute data") as pbar:
            for data in institute_data:
                future = self.executor.submit(self._add_institute, data)
                futures.append(future)

            for future in as_completed(futures):
                future.result()
                pbar.update(1)

    def _add_student(self, data):
        session = self.Session()
        try:
            student = Student(
                barcode=data['Barcode'],
                course_type=data['CourseName'],
                year=data['Year'],
                type=data['TypeofInstitute'],  # Assuming a default value
                # Add other columns as needed
            )
            session.add(student)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error adding student: {e}")
        finally:
            session.close()

    def _add_institute(self, data):
        session = self.Session()
        try:
            institute = Institute(
                sams_code=data['SAMSCode'],
                year=int(data['academic_year']),
                module=data['module'],
                name=data['InstituteName'],
                type=data['TypeofInstitute']
                
                
                # Add other columns as needed
            )
            session.add(institute)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error adding institute: {e}")
        finally:
            session.close()

    def close(self):
        self.executor.shutdown()