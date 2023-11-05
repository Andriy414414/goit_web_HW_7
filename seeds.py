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
NUMBER_GRADES = 20
fake = Faker('uk-UA')


def seed_teachers():
    for _ in range(NUMBER_TEACHERS):
        teacher = Teacher(
            fullname=fake.name()
        )
        session.add(teacher)


def seed_subjects():

    for sub in subjects:
        subject = Subject(
            name=sub,
            teacher_id=randint(1, NUMBER_TEACHERS)
        )
        session.add(subject)



def seed_groups():

    for gr in groups:
        group = Group(
            name=gr
        )
        session.add(group)



def seed_students():

    for _ in range(NUMBER_STUDENTS):
        student = Student(
            fullname=fake.name(),
            group_id=randint(1, len(groups))
        )
        session.add(student)


def seed_grades():

    start_date = datetime.strptime("2022-09-01", "%Y-%m-%d")
    end_date = datetime.strptime("2023-06-15", "%Y-%m-%d")

    def get_list_date(start: date, end: date):
        result = []
        current_date = start
        while current_date <= end:
            if current_date.isoweekday() < 6:
                result.append(current_date)
            current_date += timedelta(1)
        return result

    list_dates = get_list_date(start_date, end_date)
    n = 1
    counter = 0
    for _ in range(NUMBER_STUDENTS*NUMBER_GRADES):
        grade = Grade(
            grade=randint(1, 100),
            date_of=random.choice(list_dates),
            student_id=n,
            subject_id=randint(1, len(subjects))
        )
        counter += 1
        if counter % 20 == 0:
            n += 1
        session.add(grade)


if __name__ == "__main__":
    try:
        # seed_teachers()
        seed_subjects()
        seed_groups()
        seed_students()
        seed_grades()
        session.commit()
    except SQLAlchemyError as e:
        print(e)
        session.rollback()
    finally:
        session.close()
