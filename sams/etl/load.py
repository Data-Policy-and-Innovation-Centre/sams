from sqlalchemy import create_engine, Column, Integer, String, JSON, Enum, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import OperationalError, IntegrityError, DatabaseError
import pandas as pd
from tqdm import tqdm
import time
from loguru import logger
from sams.config import ERRMAX, RAW_DATA_DIR
from sams.etl.extract import SamsDataDownloader

Base = declarative_base()

class Institute(Base):
    __tablename__ = 'institutes'

    # Primary columns
    id = Column(Integer, primary_key=True, autoincrement=True)
    sams_code = Column(Integer,nullable=False)
    year = Column(Integer, nullable=False)
    module = Column(Enum("ITI", "Diploma", "PDIS"), nullable=False)
    name = Column(String)
    funding_source = Column(Enum("Govt.", "Pvt."))

    # Nested columns
    strength = Column(JSON)
    cutoff = Column(JSON)

    # Polymorphic identity for inheritance
    __mapper_args__ = {
        'polymorphic_on': module
    }

class ITI(Institute):
    __tablename__ = 'itis'
    id = Column(Integer, ForeignKey('institutes.id'), primary_key=True)
    trade = Column(String)  # Trade is specific to non-PDIS

    __mapper_args__ = {
        'polymorphic_identity': 'ITI',
    }

class Diploma(Institute):
    __tablename__ = 'diplomas'
    id = Column(Integer, ForeignKey('institutes.id'), primary_key=True)
    trade = Column(String)  # Trade is specific to non-PDIS

    __mapper_args__ = {
        'polymorphic_identity': 'Diploma',
    }

class PDIS(Institute):
    __tablename__ = 'pdis'
    id = Column(Integer, ForeignKey('institutes.id'), primary_key=True)

    __mapper_args__ = {
        'polymorphic_identity': 'PDIS',
    }


class SamsDataLoader:
    def __init__(self, db_url):
        self.engine = create_engine(db_url, echo=False, pool_size=20, max_overflow=10)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def load_institute_data(self, institute_data):
        with tqdm(total=len(institute_data), desc="Loading institute data") as pbar:
            for data in institute_data:
                self._add_institute(data)
                pbar.update(1)

    def _add_institute(self, data):
        session = self.Session()
        try:
            institute = Institute(
                sams_code=data['SAMSCode'],
                year=int(data['academic_year']),
                module=data['module'],
                name=data['InstituteName'],
                funding_source=data['TypeofInstitute'] ,         
                
                # Add other columns as needed
                strength=data['strength'],
                cutoff=data['cuttoff']
            )
            session.add(institute)
            session.commit()
        except Exception as e:
            if 'database is locked' in str(e):
                time.sleep(1)
            else:
                session.rollback()
                print(f"Error adding institute: {e}")
        finally:
            session.close()

    def close(self):
        self.executor.shutdown()

class SamsDataLoaderPandas(SamsDataLoader):
    def __init__(self, db_url):
        super().__init__(db_url)

    def load_data(self, data: pd.DataFrame, table_name: str) -> None:
        """
        Loads data from a pandas dataframe into a table in the database.

        Args:
            data (pd.DataFrame): The data to load into the database.
            table_name (str): The name of the table to load the data into.

        Returns:
            None
        """
        num_retries = 0

        while num_retries < ERRMAX:
            try:
                data.to_sql(table_name, con=self.engine, if_exists='append', index=False)
                break
            except IntegrityError as e:
                logger.error(f"Error loading data into {table_name}: {e}")
                break
            except (DatabaseError, OperationalError) as e:
                logger.error(f"Error loading data into {table_name}: {e}")
                logger.info(f"Retrying ({num_retries+1}/{ERRMAX})...")
                num_retries += 1
                time.sleep(1)
            
def main():
    loader = SamsDataLoaderPandas(f'sqlite:///{RAW_DATA_DIR}/sams.db')
    downloader = SamsDataDownloader()
    institute_data = downloader.fetch_institutes('ITI', 2022)
    loader.load_institute_data(institute_data)

if __name__ == "__main__":
    main()
    