from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_02():
    """
    SELECT s2.name, s.fullname, s.id, ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s on s.id = g.student_id
    JOIN subjects s2 ON s2.id = g.subject_id
    WHERE s2.id = 6
    GROUP BY s.fullname
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Subject.name, Student.id, Student.fullname,
                           func.round(func.avg(Grade.grade), 2).label("average_grade")) \
        .select_from(Student).join(Grade).join(Subject).filter(Subject.id == 2) \
        .group_by(Student.id, Subject.name).order_by(desc('average_grade')).limit(1).all()
    return result


def select_03():
    """
    SELECT s2.name, g2.name , ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN subjects s2 ON s2.id = g.subject_id
    JOIN students s on s.id = g.student_id
    JOIN groups g2 ON g2.id = s.group_id
    WHERE s2.id = 2
    GROUP BY g2.name, s2.name
    ORDER BY average_grade DESC
    """
    result = session.query(Subject.name, Group.name, func.round(func.avg(Grade.grade), 2).label("average_grade")) \
        .select_from(Subject).join(Grade).join(Student).join(Group).filter(Subject.id == 2) \
        .group_by(Subject.name, Group.name).order_by(desc('average_grade')).limit(3).all()
    return result


def select_04():
    """
    SELECT ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label("average_grade")) \
        .select_from(Grade).all()
    return result


def select_05():
    """
    SELECT s3.name, t.fullname
    FROM subjects s3
    JOIN teachers t ON t.id = s3.teacher_id
    WHERE t.id = 3
    """
    result = session.query(Teacher.fullname, Subject.name) \
        .select_from(Subject).join(Teacher).filter(Teacher.id == 4).all()
    return result


def select_06():
    """
    SELECT s3.fullname, g.name
    FROM students s3
    JOIN groups g ON g.id = s3.group_id
    WHERE g.id = 3
    order by s3.fullname
    """
    result = session.query(Student.fullname, Group.name) \
        .select_from(Student).join(Group).filter(Group.id == 3) \
        .order_by(Student.fullname).all()
    return result


def select_07():
    """
    SELECT s2.name, g2.name, g.grade, s.fullname
    FROM grades g
    JOIN subjects s2 ON s2.id = g.subject_id
    JOIN students s on s.id = g.student_id
    JOIN groups g2 ON g2.id = s.group_id
    WHERE g2.id = 1 AND s2.id = 1
    """
    result = session.query(Subject.name, Group.name, Grade.grade, Student.fullname) \
        .select_from(Grade).join(Student).join(Group).join(Subject).filter(Subject.id == 1, Group.id == 1) \
        .order_by(Student.fullname).all()
    return result


def select_08():
    """
    SELECT s2.name, t.fullname, ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN subjects s2 ON s2.id = g.subject_id
    JOIN teachers t ON t.id = s2.teacher_id
    WHERE t.id = 4
    GROUP BY t.fullname, s2.name
    """
    result = session.query(Teacher.fullname, Subject.name, func.round(func.avg(Grade.grade), 2).label("average_grade")) \
        .select_from(Grade).join(Subject).join(Teacher).filter(Teacher.id == 4) \
        .group_by(Teacher.fullname, Subject.name).all()
    return result


def select_09():
    """
    SELECT s.fullname, s2.name
    FROM grades g
    JOIN subjects s2 ON s2.id = g.subject_id
    JOIN students s on s.id = g.student_id
    JOIN groups g2 ON g2.id = s.group_id
    WHERE s.id = 50
    GROUP BY s.fullname, s2.name
    """
    result = session.query(Student.fullname, Subject.name) \
        .select_from(Grade).join(Student).join(Subject).filter(Student.id == 15) \
        .group_by(Student.fullname, Subject.name).all()
    return result


def select_10():
    """
    SELECT s.fullname, s2.name, t.fullname
    FROM grades g
    JOIN subjects s2 ON s2.id = g.subject_id
    JOIN students s on s.id = g.student_id
    JOIN teachers t  ON t.id = s2.teacher_id
    WHERE s.id = 49 and t.id = 1
    GROUP BY s.fullname, s2.name, t.fullname
    """
    result = session.query(Student.fullname.label("Student"), Subject.name, Teacher.fullname.label("Teacher")) \
        .select_from(Grade).join(Student).join(Subject).join(Teacher).filter(Student.id == 49, Teacher.id == 1) \
        .group_by(Student.fullname, Subject.name, Teacher.fullname).all()
    return result


def select_11():
    """
    SELECT s.fullname, t.fullname, ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN subjects s2 ON s2.id = g.subject_id
    JOIN teachers t ON t.id = s2.teacher_id
    join students s on s.id = g.student_id
    WHERE t.id = 5 and s.id = 15
    GROUP BY t.fullname, s.fullname
    """
    result = session.query(Student.fullname.label("Student"), Teacher.fullname.label("Teacher"), \
                           func.round(func.avg(Grade.grade), 2).label("average_grade")) \
        .select_from(Grade).join(Student).join(Subject).join(Teacher).filter(Student.id == 15, Teacher.id == 5) \
        .group_by(Student.fullname, Teacher.fullname).all()
    return result


def select_12():
    """
    select s.id, s.fullname, g.grade, g.date_of
    from grades g
    join students s on g.student_id = s.id
    where g.subject_id = 5 and s.group_id = 1 and g.date_of = (
        select max(date_of)
        from grades g2
        join students s2 on s2.id=g2.student_id
        where g2.subject_id = 5 and s2.group_id = 1
    );
    """
    subquery = (select(func.max(Grade.date_of)).join(Student) \
        .filter(and_(Grade.subject_id == 5, Student.group_id == 1))) \
        .scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.date_of) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subject_id == 5, Student.group_id == 1, Grade.date_of == subquery)).all()

    return result


if __name__ == '__main__':
    print(f'\n 5 студентів із найбільшим середнім балом з усіх предметів: \n {select_01()}')
    print(f'\n Cтудент із найвищим середнім балом з певного предмета: \n {select_02()}')
    print(f'\n Cередній бал у групах з певного предмета: \n {select_03()}')
    print(f'\n Cередній бал на потоці (по всій таблиці оцінок): \n {select_04()}')
    print(f'\n Курси, які читає певний викладач: \n {select_05()}')
    print(f'\n Список студентів у певній групі: \n {select_06()}')
    print(f'\n Оцінки студентів у окремій групі з певного предмета: \n {select_07()}')
    print(f'\n Середній бал, який ставить певний викладач зі своїх предметів: \n {select_08()}')
    print(f'\n Список курсів, які відвідує студент: \n {select_09()}')
    print(f'\n Список курсів, які певному студенту читає певний викладач: \n {select_10()}')
    print(f'\n Середній бал, який певний викладач ставить певному студентові: \n {select_11()}')
    print(f'\n Оцінки студентів у певній групі з певного предмета на останньому занятті: \n {select_12()}')
