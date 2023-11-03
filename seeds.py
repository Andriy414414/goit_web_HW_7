import random
from datetime import datetime, date, timedelta
from random import randint, choice
from faker import Faker
from sqlalchemy.exc import SQLAlchemyError

from conf.db import session
from conf.models import Teacher, Group, Student, Subject, Grade

subjects = ["Math", "Disc Math", "Line Algebra", "Programming", "Theory University", "History", "English", "IZO"]
groups = ["A331", "TP001", "BC143"]
NUMBER_TEACHERS = 5
NUMBER_STUDENTS = 50
fake = Faker('uk-UA')


def seed_teachers():
    for _ in range(NUMBER_TEACHERS):
        teacher = Teacher(
            fullname=fake.fullname()
        )
        session.add(teacher)


def seed_subjects():
    teachers = session.query(Teacher).all()

    for _ in range(len(subjects)):
        n = 1
        subject = Subject(
            name=subjects[n],
            teacher_id=random.choice(teachers).id
        )


def seed_groups():
    sql = "INSERT INTO groups(name) VALUES(?);"
    cur.executemany(sql, zip(groups, ))


def seed_students():
    students = [fake.name() for _ in range(NUMBER_STUDENTS)]


def seed_grades():
    start_date = datetime.strptime("2022-09-01", "%Y-%m-%d")
    end_date = datetime.strptime("2023-06-15", "%Y-%m-%d")
    sql = "INSERT INTO grades(subject_id, student_id, grade, date_of) VALUES(?, ?, ?, ?);"

    def get_list_date(start: date, end: date):
        result = []
        current_date = start
        while current_date <= end:
            if current_date.isoweekday() < 6:
                result.append(current_date)
            current_date += timedelta(1)
        return result

    list_dates = get_list_date(start_date, end_date)

    grades = []

    for day in list_dates:
        random_subjects = randint(1, len(subjects))
        random_students = [randint(1, NUMBER_STUDENTS) for _ in range(5)]
        for student in random_students:
            grades.append((random_subjects, student, randint(1, 12), day.date()))

    cur.executemany(sql, grades)


if __name__ == "__main__":
    try:
        seed_teachers()
        seed_subjects()
        seed_groups()
        seed_students()
        seed_grades()
        connect.commit()
    except sqlite3.Error as error:
        pprint(error)
    finally:
        connect.close()
