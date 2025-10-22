class Endpoints:
    BASE_URL = "https://api.samsodisha.gov.in/api/"

    def get_student_data(self):
        return f"{self.BASE_URL}GetDPICStudentData"

    def get_institute_data(self):
        return f"{self.BASE_URL}GetDPICInstituteData"
    
    def get_plus2_student_data(self):
        return f"{self.BASE_URL}GetHSSStudentData"
    
    def get_deg_student_data(self):
        return f"{self.BASE_URL}GETDEGStudentData"

