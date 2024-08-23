from sqlalchemy import create_engine, Column, Integer, String, Table, MetaData
from sqlalchemy.orm import sessionmaker
from sams.config import PROCESSED_DATA_DIR, RAW_DATA_DIR

class SamsDataLoader:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.session = sessionmaker(bind=self.engine)()
        self.metadata = MetaData()

    def define_tables(self):
        self.students_table = Table('students', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String),
            Column('program', String),
            Column('year', Integer),
            # Add other relevant columns here
        )

        self.institutes_table = Table('institutes', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String),
            Column('program', String),
            Column('year', Integer),
            # Add other relevant columns here
        )

        # Create the tables in the database
        self.metadata.create_all(self.engine)

    def load_data(self, data, table):
        for year, programs in data.items():
            for program, entries in programs.items():
                for entry in entries:
                    insert_stmt = table.insert().values(
                        id=entry['id'],
                        name=entry['name'],
                        program=program,
                        year=year,
                        # Add other relevant fields here
                    )
                    self.session.execute(insert_stmt)

    def commit(self):
        self.session.commit()

    def close(self):
        self.session.close()