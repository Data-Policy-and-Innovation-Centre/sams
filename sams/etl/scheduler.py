import time
import schedule

class SamsDataScheduler:
    def __init__(self, downloader, loader):
        self.downloader = downloader
        self.loader = loader

    def scheduled_task(self, programs, years):
        print("Starting scheduled task...")

        # Download the data
        student_data = self.downloader.download_student_data(programs, years)
        institute_data = self.downloader.download_institute_data(programs, years)

        # Define the tables
        self.loader.define_tables()

        # Load the data into the database
        self.loader.load_data(student_data, self.loader.students_table)
        self.loader.load_data(institute_data, self.loader.institutes_table)

        # Commit the transaction
        self.loader.commit()

        print("Scheduled task completed!")

    def schedule_download_and_load(self, programs, years, interval_minutes):
        # Schedule the task to run at a fixed interval
        schedule.every(interval_minutes).minutes.do(self.scheduled_task, programs, years)

        print(f"Scheduled to run every {interval_minutes} minutes.")

        while True:
            schedule.run_pending()
            time.sleep(1)