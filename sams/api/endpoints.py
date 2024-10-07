class Endpoints:
    BASE_URL = "https://api.samsodisha.gov.in/api/"

    def get_student_data(self):
        return f"{self.BASE_URL}GetDPICStudentData"

    def get_institute_data(self):
        return f"{self.BASE_URL}GetDPICInstituteData"
