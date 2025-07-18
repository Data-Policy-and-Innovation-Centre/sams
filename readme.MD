
# SAMS

The **Student Academic Management System (SAMS)** is a data pipeline and analytics project designed to manage, process, and analyze large-scale student-level data across various academic programs in Odisha. This includes Industrial Training Institutes (ITI), Diploma programs, Post Diploma in Industrial Safety (PDIS), and Higher Secondary Schools (HSS).

---

## Prerequisites

Before using SAMS, ensure you have **Conda** installed and available in your system's PATH.

---

## Setting up the Environment

To set up the development environment:

1. Open a terminal and navigate to the root directory of the SAMS repository.
2. Run the following command to create the Conda environment:

   ```bash
   make create_env
   ```

   This will create a new Conda environment named `skills`.

3. Activate the environment:

   ```bash
   conda activate skills
   ```

---

## Running the Project

To build the dataset and initialize the database:

```bash
make dataset
```

This will load and transform raw inputs into a structured format stored in a local SQLite database.

---



## Database Structure

## Database Structure

SAMS is built on a **SQLite3** database that contains two main tables:

- **`students`**: Holds information for each student, such as their unique ID (`barcode`), gender, academic year, admission and enrollment status. Some fields also store extra details in JSON format (e.g., admission options or compartment subjects).

- **`institutes`**: Contains data about each institute, including its name, type, location, student strength, and cutoff marks.

Each student record includes a `module` field to indicate whether the student belongs to ITI, HSS, Diploma, or PDIS. These two tables are connected through fields like `barcode` or `sams_code`.

---

## Database Schema

The diagram below shows how the tables are structured and related:

![Schema Diagram](output/figures/schema_graph.png)

---


## ITI Module Summary

A broader‑level view of the Industrial Training Institutes (ITI) da

### Variable Descriptions

| Variable             | Description                                                                  |
|----------------------|------------------------------------------------------------------------------|
| `barcode`            | Encrypted student ID (SHA‑256)                                               |
| `gender`             | Student gender (`Male` / `Female`)                                           |
| `academic_year`      | Application year (2017–2024)                                                 |
| `course_period`      | Duration of the course (e.g. `2 Years`)                                      |
| `total_marks`        | Maximum possible score                                                       |
| `secured_marks`      | Marks obtained                                                               |
| `percentage`         | Percentage score                                                             |
| `social_category`    | Category (`GEN`, `OBC/SEBC`, `SC`, `ST`)                                     |
| `state` / `district` | Student’s home state and district                                            |
| `option_details`     | JSON array of {`Option_No`, `Applied Institute`, `Applied Trade`, `Reported Institute`, `SAMSCode`, etc. } |

-   **ITI applications (2017–2024):** Recorded from 34,702 in 2017 to 83,958 in 2024, peaking at 92,085 in 2023.
---

### 1. Overview

- **Total Records:** 2,218,951  
- **Academic Year** 2017-2024
- **Key Missing Fields** (count of missing values):
  - `course_period`, `reported_institute`, `reported_branch_or_trade`, `institute_district`, `type_of_institute`: 1,016,232 each  
  - `Phase`, `Option_No`, `Status`: 280,524 each  

---

### 2. Admission & Enrollment

| Category           | Yes       | No / Other | Rate (%) |
|--------------------|-----------|------------|----------|
| Enrollment Status  | 1,395,890 | 823,061    | 62.9     |
| Admission Status   | 1,202,719 | 1,016,232  | 54.2     |

---

### 3. Key Categorical Attributes

| Variable              | Most Frequent            | Frequency (%) | Observations |
|-----------------------|--------------------------|---------------|--------------|
| Gender                | Male                     | 86.47         | 2,218,951    |
| State                 | Odisha                   | 97.96         | 2,218,951    |
| District              | Ganjam                   | 17.92         | 2,218,951    |
| Social Category       | OBC/SEBC                 | 34.23         | 2,218,951    |
| Highest Qualification | 10th                     | 65.10         | 2,218,951    |
| Course Period         | 2 Years                  | 81.46 (filled)| 1,202,719    |
| Reported Institute    | ITI Berhampur, Ganjam    | 9.02 (filled) | 1,202,719    |
| Phase (Round)         | 1                        | 74.32         | 1,938,427    |
| Option_No             | 1                        | 16.52         | 1,938,427    |
| Status                | Opted                    | 100.00        | 1,938,427    |

---

## HSS Module Summary

A concise overview of the Higher Secondary School (HSS) data.


### Variable Descriptions

| Variable                     | Description                                                           |
|------------------------------|-----------------------------------------------------------------------|
| `barcode`                    | Encrypted student ID (SHA‑256)                                        |
| `academic_year`              | Application year (2018–2024)                                          |       
| `gender`                     | Student gender (`Male` / `Female`)                                    |
| `annual_income`              | Family income bracket (INR)                                           |
| `total_marks`                | Maximum exam marks (typically 600)                                    |
| `secured_marks`              | Marks obtained                                                        |
| `percentage`                 | Percentage score                                                      |
| `social_category`            | Category (`GEN`, `OBC/SEBC`, `SC`, `ST`)                              |
| `state` / `district`         | Student’s home state and district                                     |
| `hss_option_details`         | JSON array of {`Option No`, `Reported Institute`, `Applied Institute`, `AdmissionStatus`, `Applied Program`,etc. }       |
| `hss_compartments`          | JSON array of {`Subject`, `Fail Mark`, `Pass Mark`}           |

-   **HSS applications (2019–2024):** Recorded from 390,433 in 2019 to 537,230 in 2024, peaking at 573,911 in 2022.
    

### 1. Overview

- **Total Records:** 3,453,401  
- **Major Missing Fields**:
  - `social_category`: 4,91,991  
  - `gender`: 207  
  - `address`: 95  
  - `pin_code`: 98  

**Compartmental Status**  
| Status | Count    |
|--------|----------|
| NO     | 3,443,600|
| YES    | 9,801    |

---

### 2. Academic Performance

| Variable       | Observations | Mean   | Median | Std Dev | 25%   | 75%   |
|----------------|--------------|--------|--------|---------|-------|-------|
| Total Marks    | 3,453,401    | 596.13 | 600.0  | 33.26   | 600.0 | 600.0 |
| Secured Marks  | 3,453,401    | 329.60 | 322.0  | 88.22   | 258.0 | 394.0 |
| Percentage     | 3,453,401    | 55.57  | 54.17  | 34.41   | 43.33 | 66.0  |

---

### 3. Key Categorical Attributes

| Variable                           | Most Frequent                                     | Frequency (%) | Observations |
|------------------------------------|---------------------------------------------------|---------------|--------------|
| Gender                             | Female                                            | 50.25         | 3,453,194    |
| Annual Income                      | 0 – 1,50,000 INR                                  | 74.00         | 3,453,401    |
| State                              | Odisha                                            | 99.86         | 3,453,401    |
| District                           | Ganjam                                            | 7.80          | 3,453,401    |
| Social Category                    | General                                           | 27.53         | 2,961,410    |
| Highest Qualification              | 10th                                              | 100.00        | 3,453,401    |
| Board Exam Name (Qualification)    | BSE Orissa (2001 +)                              | 94.78         | 3,453,401    |
| Examination Type                   | Annual                                            | 98.69         | 3,453,401    |
| Compartmental Status               | NO                                                | 99.72         | 3,453,401    |

---

### HSS Option Details (Exploded View)

Each student may have multiple admission options, yielding over 18 million rows.

#### 1. Overview

- **Total Rows:** 18,379,224  
- **Columns:** `barcode`, `ReportedInstitute`, `SAMSCode`, `Stream`, `InstituteDistrict`, `InstituteBlock`, `TypeofInstitute`, `Phase`, `Year`, `AdmissionStatus`, `OptionNo`,  `COMPSubject`, `COMPFailMark`, `COMPPassMark`
- **Sparse Fields:**  
  - `ReportedInstitute`, `Stream`, `SAMSCode`, etc.: 23,471 missing (0.13%)  
  - `COMPSubject`, `COMPFailMark`, `COMPPassMark`: present only in 110,251 rows  

#### 2. Compartmental Marks

| Variable       | Observations | Mean   | Median | Std Dev | 25%   | 75%   |
|----------------|--------------|--------|--------|---------|-------|-------|
| COMPFailMark   | 110,251      | 21.53  | 21.0   | 9.58    | 17.0  | 26.0  |
| COMPPassMark   | 110,251      | 38.72  | 33.0   | 19.31   | 30.0  | 40.0  |

#### 3. Admission Preferences

| Variable           | Most Frequent                    | Frequency (%) | Observations  |
|--------------------|-----------------------------------|---------------|---------------|
| Stream             | Arts                              | 68.31         | 18,355,753    |
| Institute District | Ganjam                            | 8.10          | 18,355,753    |
| Institute Block    | Bhubaneswar (MC)                  | 4.61          | 18,355,753    |
| Type of Institute  | Non‑Govt Aided (488 categories)   | 49.52         | 18,355,753    |
| Phase              | 1                                 | 88.66         | 18,355,753    |
| Year               | 2023                              | 16.97         | 18,355,753    |
| Academic Year      | 2023                              | 16.95         | 18,379,224    |
| COMPSubject        | MATHEMATICS                       | 32.35         | 110,251       |

---

This streamlined summary delivers key metrics and patterns, highlighting data completeness, student demographics, academic performance, and preference structures across ITI and HSS modules.


---

## ITI Pipleine:

```mermaid
flowchart LR

  subgraph "SAMS Database"
    A1[iti_raw]
    A2[diploma_raw]
    A3[iti_institutes_raw]
    A4[diploma_institutes_raw]
    A5[sams_address_raw_df]
  end

  %% Enrollment
  A1 --> B1[iti_enrollment]
  A2 --> B2[diploma_enrollment]

  %% Address cleaning & geocoding
  A5 --> C1[sams_address_clean_df]
  C1 --> D1[geocodes_df]
  D1 --> E1[geocoded_iti_enrollment]
  D1 --> E2[geocoded_diploma_enrollment]

  %% ITI marks, cutoffs, and non‑geocoded students
  B1 --> F1[iti_marks]
  B1 --> F2[save_nongeocoded_iti_students]
  F1 --> G1[iti_marks_and_cutoffs]
  G1 --> H1[save.iti_marks_and_cutoffs]

  %% Diploma marks, cutoffs, and non‑geocoded students
  B2 --> I1[diploma_marks]
  B2 --> I2[save_nongeocoded_diploma_students]
  I1 --> J1[diploma_marks_and_cutoffs]
  J1 --> K1[save.diploma_marks_and_cutoffs]

  %% Institutes pipelines
  A3 --> L1[iti_institutes_enrollments]
  A3 --> L2[iti_institutes_cutoffs]
  A3 --> L3[iti_institutes_strength]
  L1 & L2 & L3 --> M1[save_interim_iti_institutes]

  A4 --> N1[diploma_institutes_enrollments]
  A4 --> N2[diploma_institutes_strength]
  N1 & N2 --> O1[save_interim_diploma_institutes]

  %% Vacancies
  L2 --> P1[iti_vacancies]
  P1 --> Q1[save.iti_vacancies]


