from sams.api.client import SAMSClient
from sams.api.exceptions import APIError
from requests import HTTPError, ConnectionError
import os
from loguru import logger
from sams.config import ERRMAX, STUDENT, INSTITUTE, LOGS
import pandas as pd
from tqdm import tqdm


class SamsDataDownloader:
    def __init__(self, client=None):
        if not client:
            self.api_client = SAMSClient()
        else:
            self.api_client = client

    def fetch_students(
        self, module: str, academic_year: int, pandify=True
    ) -> pd.DataFrame | list:
        """
        Fetches student data from the SAMS API for the given academic year,
        module and source of fund.

        Args:
            academic_year (int): The academic year for which to fetch the data.
            module (str): The module for which to fetch the data.
            pandify (bool, default True): If True, returns a pandas DataFrame.
                Otherwise, returns a list of dictionaries.

        Returns:
            pd.DataFrame or list: The fetched data.
        """
        academic_year = self._check_student_data_params(academic_year, module)

        expected_records = self._get_records(
            "students", academic_year, module, count=True
        )

        if module in ["ITI", "Diploma"]:
            data = self._get_students_iti_diploma(academic_year, module)
        else:
            data = self._get_records("students", academic_year, module)

        if len(data) < expected_records:
            logger.warning(
                f"Expected {expected_records} records, but got {len(data)} records."
            )

        try:
            info = f"""\nStudent data downloaded for module {module}, academic year {academic_year}. 
            \n.Num fields: {len(data[0])} \n.Num records: {len(data)} \n.Num distinct records: { len(set(map(lambda item: tuple(sorted(item.items())), data)))} \n.Expected records: {expected_records}\n\n\n"""
            logger.info(info)
        except IndexError as e:
            logger.error(
                f"Student data missing for module {module}, academic year {academic_year}."
            )
        except TypeError as t:
            info = f"""\nStudent data downloaded for module {module}, academic year {academic_year}. 
            \n.Num fields: {len(data[0])} \n.Num records: {len(data)} \n.Expected records: {expected_records}"""
            logger.info(info)

        if pandify:
            df = pd.DataFrame(data)
            df["module"] = module
            df["academic_year"] = academic_year
            return df
        else:
            for item in data:
                item["module"] = module
                item["academic_year"] = academic_year
            return data

    def fetch_institutes(
        self, module: str, academic_year: int, admission_type: int = None, pandify=False
    ) -> list | pd.DataFrame:
        academic_year = self._check_institute_data_params(
            academic_year, module, admission_type
        )

        expected_records = self._get_records(
            "institutes",
            academic_year,
            module,
            admission_type=admission_type,
            count=True,
        )

        data = self._get_records(
            "institutes", academic_year, module, admission_type=admission_type
        )

        if len(data) < expected_records:
            logger.warning(
                f"Expected {expected_records} records, but got {len(data)} records."
            )

        try:
            info = f"""\nInstitute data downloaded for module {module}, academic year {academic_year}, admission type {admission_type}.
            \n.Num fields: {len(data[0])} \n.Num records: {len(data)} \n.Num distinct records: { len(set(map(lambda item: tuple(sorted(item.items())), data)))} \n.Expected records: {expected_records}"""
            logger.info(info)
        except IndexError as e:
            logger.error(
                f"Institute data missing for module {module}, academic year {academic_year}, admission type {admission_type}."
            )
        except TypeError:
            info = f"""\nInstitute data downloaded for module {module}, academic year {academic_year}, admission type {admission_type}.
            \n.Num fields: {len(data[0])} \n.Num records: {len(data)} \n.Expected records: {expected_records}"""
            logger.info(info)

        if pandify:
            df = pd.DataFrame(data)
            df["module"] = module
            df["academic_year"] = academic_year
            df["admission_type"] = admission_type
            return df
        else:
            for item in data:
                item["module"] = module
                item["academic_year"] = academic_year
                item["admission_type"] = admission_type
            return data

    def _get_students_iti_diploma(self, academic_year: int, module: str) -> list:
        """
        Downloads student data for ITI and Diploma from SAMS API.

        Args:
            academic_year (int): The academic year for which to fetch the data.
            module (str): The module for which to fetch the data.

        Returns:
            list: The downloaded data.
        """

        page = 1
        data = []

        while True:
            records = self._get_records(
                table_name="students",
                module=module,
                academic_year=academic_year,
                page_number=page,
            )

            if len(records) == 0:
                break

            data.extend(records)
            page += 1

        return data

    def _get_records(
        self,
        table_name: str,
        academic_year: int,
        module: str,
        admission_type: int = None,
        page_number=1,
        count=False,
    ) -> int | list:
        """
        Fetches the expected number of student records, or the actual list of records from SAMS API for the given academic year, module and source of fund.

        Args:
            table (str): The table for which to fetch the data.
            academic_year (int): The academic year for which to fetch the data.
            module (str): The module for which to fetch the data.
            page_number (int, optional): The page number to fetch.
            count (bool, optional): If True, returns the expected number of records. Otherwise returns a list consisting of the actual records.

        Returns:
            int | list : The expected number of student records or a list of records in JSON format.
        """
        if table_name not in ["students", "institutes"]:
            raise ValueError(f"Invalid table name: {table_name}")

        retries = 0
        records = []

        while retries < ERRMAX:
            try:
                if table_name == "students":
                    records = self.api_client.get_student_data(
                        module=module,
                        academic_year=academic_year,
                        page_number=page_number,
                        count=count,
                    )
                else:
                    records = self.api_client.get_institute_data(
                        module=module,
                        academic_year=academic_year,
                        admission_type=admission_type,
                        count=count,
                    )
                break
            except (APIError, HTTPError, ConnectionError) as e:
                logger.error(f"API Error: {e}")
                logger.error(f"Retrying...({retries+1}/{ERRMAX})")
                retries += 1
                continue

        if not records and count:
            records = 0

        return records

    def _check_student_data_params(self, academic_year: int, module: str) -> int:
        """
        Checks if the given academic year, module and source of fund are valid,
        and if not, adjusts them to the nearest valid values.

        Args:
            academic_year (int): The academic year for which to fetch the data.
            module (str): The module for which to fetch the data.

        Returns:
            int: The adjusted academic year.
        """
        if module not in ["ITI", "Diploma", "PDIS"]:
            raise ValueError("Module must be either 'ITI', 'PDIS' or 'Diploma'. ")

        if (
            academic_year < STUDENT[module]["yearmin"]
            or academic_year > STUDENT[module]["yearmax"]
        ):
            logger.warning(
                f"Data from Academic year {academic_year} is not available for {module}. It must be between {STUDENT[module]['yearmin']} and {STUDENT[module]['yearmax']}. "
            )
            academic_year = min(
                STUDENT[module]["yearmax"],
                max(STUDENT[module]["yearmin"], academic_year),
            )
            logger.warning(f"Adjusting year to {academic_year} for {module}. ")

        return academic_year

    def _check_institute_data_params(
        self, academic_year: int, module: str, admission_type: int = None
    ) -> int:
        """
        Checks if the given academic year, module and admission type are valid,
        and if not, adjusts them to the nearest valid values.

        Args:
            academic_year (int): The academic year for which to fetch the data.
            module (str): The module for which to fetch the data.
            admission_type (int, optional): The admission type for which to fetch the data. Defaults to None.

        Returns:
            int: The adjusted academic year.
        """
        if module not in ["ITI", "Diploma", "PDIS"]:
            raise ValueError("Module must be either 'ITI', 'PDIS' or 'Diploma'. ")

        if module == "Diploma" and admission_type not in [1, 2]:
            raise ValueError(
                "Admission type must be either 1 (for Fresh Entry) or 2 (for Lateral Entry). "
            )

        if (
            academic_year < INSTITUTE[module]["yearmin"]
            or academic_year > INSTITUTE[module]["yearmax"]
        ):
            logger.warning(
                f"Data from Academic year {academic_year} is not available for {module}. It must be between {INSTITUTE[module]['yearmin']} and {INSTITUTE[module]['yearmax']}. "
            )
            academic_year = min(
                INSTITUTE[module]["yearmax"],
                max(INSTITUTE[module]["yearmin"], academic_year),
            )
            logger.warning(f"Adjusting year to {academic_year} for {module}. ")

        return academic_year

    def update_total_records(self) -> None:
        """
        Updates the total number of student and institute records by downloading them from SAMS API.

        This method downloads the total number of student and institute records for each module from SAMS API, and updates the counts in a csv file.

        The file is saved in the logs directory with the name "student_count.csv" and "institute_count.csv" respectively.

        This method is useful for updating the total number of records periodically, so that the progress bars are accurate.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        # Remove all existing log handlers
        for handler_id in list(logger._core.handlers.keys()):
            logger.remove(handler_id)

        # Add a new log handler for downloading student data
        log_file_id = logger.add(
            os.path.join(LOGS, "total_records.log"),
            mode="w",
            format="{time} {level} {message}",
            level="INFO",
        )

        # Set up counter
        student_counter = pd.DataFrame(columns=["module", "academic_year", "count"])
        institute_counter = pd.DataFrame(
            columns=["module", "academic_year", "admission_type", "count"]
        )

        # Students
        student_counter = self._update_total_records(
            student_counter, STUDENT, table_name="students"
        )

        # Institutes
        institute_counter = self._update_total_records(
            institute_counter, INSTITUTE, table_name="institutes"
        )

        # Dump counts in json file
        student_counter.to_csv(os.path.join(LOGS, "students_count.csv"), index=False)
        institute_counter.to_csv(
            os.path.join(LOGS, "institutes_count.csv"), index=False
        )

        # Close the log handler
        logger.remove(log_file_id)
        logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
        logger.info("Total records updated.")

    def _update_total_records(
        self, counter: pd.DataFrame, metadict: dict, table_name: str
    ) -> pd.DataFrame:
        if table_name not in ["students", "institutes"]:
            raise ValueError("type must be either 'students' or 'institutes'")

        for module, metadata in metadict.items():
            for year in range(metadata["yearmin"], metadata["yearmax"] + 1):
                retries = 0
                while retries < ERRMAX:
                    try:
                        if table_name == "students":
                            count = self.api_client.get_student_data(
                                module, year, page_number=1, count=True
                            )
                            new_row = pd.DataFrame(
                                [
                                    {
                                        "module": module,
                                        "academic_year": year,
                                        "count": count,
                                    }
                                ]
                            )
                            counter = pd.concat([counter, new_row], ignore_index=True)
                        else:
                            if module == "Diploma":
                                for admission_type in [1, 2]:
                                    count = self.api_client.get_institute_data(
                                        module, year, admission_type, count=True
                                    )
                                    new_row = pd.DataFrame(
                                        [
                                            {
                                                "module": module,
                                                "academic_year": year,
                                                "admission_type": admission_type,
                                                "count": count,
                                            }
                                        ]
                                    )
                                    counter = pd.concat(
                                        [counter, new_row], ignore_index=True
                                    )
                            else:
                                count = self.api_client.get_institute_data(
                                    module, year, count=True
                                )
                                new_row = pd.DataFrame(
                                    [
                                        {
                                            "module": module,
                                            "academic_year": year,
                                            "admission_type": None,
                                            "count": count,
                                        }
                                    ]
                                )
                                counter = pd.concat(
                                    [counter, new_row], ignore_index=True
                                )

                        break
                    except APIError as e:
                        retries += 1
                        if retries >= ERRMAX:
                            logger.error(
                                f"Data download failed for {module} {year} after {ERRMAX} retries. Skipping..."
                            )
                            break
                        else:
                            continue
        return counter


def main():
    """
    Main function that downloads student data from the SAMS API and saves it to an Excel file.
    """

    downloader = SamsDataDownloader()
    df = downloader.fetch_institutes("ITI", 2020, pandify=True)
    print(df.columns)


if __name__ == "__main__":
    main()
