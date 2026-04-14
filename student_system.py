import sqlite3
import pandas as pd
import random
from faker import Faker
import logging
import re

# =========================
# DATABASE SETUP
# =========================
conn = sqlite3.connect("student_records.db")
cursor = conn.cursor()

cursor.executescript("""
DROP TABLE IF EXISTS Attendance;
DROP TABLE IF EXISTS Grades;
DROP TABLE IF EXISTS Enrollments;
DROP TABLE IF EXISTS Courses;
DROP TABLE IF EXISTS Students;

CREATE TABLE Students (
    student_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    dob TEXT,
    major TEXT
);

CREATE TABLE Courses (
    course_id INTEGER PRIMARY KEY,
    course_name TEXT NOT NULL,
    credits INTEGER
);

CREATE TABLE Enrollments (
    enrollment_id INTEGER PRIMARY KEY,
    student_id INTEGER,
    course_id INTEGER
);

CREATE TABLE Grades (
    grade_id INTEGER PRIMARY KEY,
    student_id INTEGER,
    course_id INTEGER,
    score REAL
);

CREATE TABLE Attendance (
    attendance_id INTEGER PRIMARY KEY,
    student_id INTEGER,
    course_id INTEGER,
    date TEXT,
    status TEXT
);
""")

conn.commit()
print("Database created!")

# =========================
# DATA GENERATION
# =========================
fake = Faker()
students = []

for i in range(1, 201):
    students.append([
        i,
        fake.name(),
        fake.email(),
        fake.date_of_birth(minimum_age=18, maximum_age=30),
        fake.job()
    ])

students_df = pd.DataFrame(students, columns=["student_id", "name", "email", "dob", "major"])

courses = []
for i in range(1, 26):
    courses.append([i, f"Course {i}", random.randint(3, 5)])

courses_df = pd.DataFrame(courses, columns=["course_id", "course_name", "credits"])

# =========================
# ENROLLMENTS
# =========================
enrollments = []
enrollment_id = 1

for student_id in students_df["student_id"]:
    selected_courses = random.sample(list(courses_df["course_id"]), k=random.randint(10, 20))
    for course_id in selected_courses:
        enrollments.append([enrollment_id, student_id, course_id])
        enrollment_id += 1

enrollments_df = pd.DataFrame(enrollments, columns=["enrollment_id", "student_id", "course_id"])

# =========================
# GRADES
# =========================
grades = []
grade_id = 1

for _, row in enrollments_df.iterrows():
    grades.append([
        grade_id,
        row["student_id"],
        row["course_id"],
        round(random.uniform(40, 100), 2)
    ])
    grade_id += 1

grades_df = pd.DataFrame(grades, columns=["grade_id", "student_id", "course_id", "score"])

# =========================
# ATTENDANCE
# =========================
attendance = []
attendance_id = 1

for _, row in enrollments_df.iterrows():
    for _ in range(random.randint(5, 10)):
        attendance.append([
            attendance_id,
            row["student_id"],
            row["course_id"],
            fake.date_this_year(),
            random.choice(["Present", "Absent"])
        ])
        attendance_id += 1

attendance_df = pd.DataFrame(attendance, columns=["attendance_id", "student_id", "course_id", "date", "status"])

# =========================
# SAVE FILES
# =========================
students_df.to_csv("students.csv", index=False)
courses_df.to_excel("courses.xlsx", index=False)
enrollments_df.to_csv("enrollments.csv", index=False)
grades_df.to_csv("grades.csv", index=False)
attendance_df.to_json("attendance.json", orient="records")

print("Files saved!")

# =========================
# ETL PROCESS
# =========================
logging.basicConfig(filename="etl.log", level=logging.INFO)

students_df = pd.read_csv("students.csv")
courses_df = pd.read_excel("courses.xlsx")
enrollments_df = pd.read_csv("enrollments.csv")
grades_df = pd.read_csv("grades.csv")
attendance_df = pd.read_json("attendance.json")

# Cleaning
students_df["email"] = students_df["email"].str.lower().str.strip()
students_df.drop_duplicates(inplace=True)

# Email validation
def is_valid_email(email):
    return re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email)

students_df = students_df[students_df["email"].apply(is_valid_email)]

# GPA calculation
def calculate_gpa(score):
    if score >= 75:
        return 4.0
    elif score >= 65:
        return 3.0
    elif score >= 50:
        return 2.0
    else:
        return 1.0

grades_df["GPA"] = grades_df["score"].apply(calculate_gpa)

# =========================
# LOAD TO DATABASE
# =========================
conn = sqlite3.connect("student_records.db")

students_df.to_sql("Students", conn, if_exists="append", index=False)
courses_df.to_sql("Courses", conn, if_exists="append", index=False)
enrollments_df.to_sql("Enrollments", conn, if_exists="append", index=False)
grades_df.drop(columns=["GPA"]).to_sql("Grades", conn, if_exists="append", index=False)
attendance_df.to_sql("Attendance", conn, if_exists="append", index=False)

conn.commit()
conn.close()

print("Data loaded into database!")

# =========================
# SAMPLE QUERY
# =========================
conn = sqlite3.connect("student_records.db")

result = pd.read_sql("""
SELECT student_id, AVG(score) as avg_score
FROM Grades
GROUP BY student_id
ORDER BY avg_score DESC
LIMIT 10
""", conn)

print(result)

conn.close()