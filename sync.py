import datetime
from logging import Logger
import os
import sys
import urllib.request
import json
import asyncio
import asyncpg
from dotenv import load_dotenv
from dataclasses import dataclass
from logging.handlers import TimedRotatingFileHandler
import logging
import pathlib
from datetime import datetime

current_path = pathlib.Path(__file__).parent.resolve()

logging.basicConfig(filename=f"{current_path}//log//{datetime.now().year}-{datetime.now().month}-{datetime.now().day}.log",
                    encoding='utf-8', level=logging.DEBUG)

DAAD_JSON = "https://www2.daad.de/deutschland/studienangebote/international-programmes/api/solr/en/search.json"


@dataclass
class Environment:
    user: str
    password: str
    database: str
    host: str


def load_json(json_url):
    with urllib.request.urlopen(json_url) as url:
        data = json.loads(url.read().decode())
    return data


async def sync_university(data, env: Environment):
    conn = await asyncpg.connect(user=env.user, password=env.password,
                                 database=env.database, host=env.host)

    logging.info('=========Start syncing university=========')
    try:
        uni_rows = await conn.fetch('SELECT id, name_en, name_ch, city, is_from_daad, is_tu9, is_u15, qs_ranking, created_at, updated_at, link FROM university')
        max_id = await conn.fetchval('SELECT MAX(id) FROM university')

        # data from db
        dicts = [dict(row) for row in uni_rows]

        new_uni = []

        for c in data["courses"]:
            find_dict = next(
                (item for item in dicts if item["name_en"] == c["academy"]), None)
            if not find_dict and c["academy"] not in [t[0] for t in new_uni]:
                new_uni.append((c["academy"], c["city"]))

        if new_uni:
            # prepare rows for insertation
            id = int(max_id) + 1
            rows = []
            for uni in new_uni:
                name_en, city = uni
                row = (id, name_en, '', city, True, False, False,
                       None, datetime.now(), None, '')
                rows.append(row)
                id += 1

            statement = "INSERT INTO university (id, name_en, name_ch, city, is_from_daad, is_tu9, is_u15, qs_ranking, created_at, updated_at, link) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);"
            await conn.executemany(statement, rows)
            logging.info("New university:")
            logging.info(rows)
        else:
            logging.info('No new university')

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logging.error(f"{e} line: {exc_tb.tb_lineno}")
    finally:
        await conn.close()

    logging.info('=========End syncing university  =========')


async def sync_course_create(daad_data, env: Environment):
    conn = await asyncpg.connect(user=env.user, password=env.password,
                                 database=env.database, host=env.host)

    logging.info('=========Start creating new course=========')

    try:
        course_rows = await conn.fetch("""SELECT c.id, c.university_id, u.name_en as university_name, c.course_type, c.name_en, c.name_en_short, c.name_ch, c.name_ch_short, 
      c.tuition_fees, c.beginning, c.subject, c.daadlink, c.is_elearning, c.application_deadline, 
      c.is_complete_online_possible, c.programme_duration 
      FROM course as c
      LEFT JOIN university as u on c.university_id = u.id
      """)
        uni_rows = await conn.fetch('SELECT id, name_en FROM university')
        max_course_language_id = await conn.fetchval('SELECT MAX(id) FROM courses_languages')

        # data from db
        course_dicts = [dict(row) for row in course_rows]
        university_dicts = [dict(row) for row in uni_rows]

        new_courses = []

        for c in daad_data["courses"]:
            find_course = next(
                (item for item in course_dicts if item["id"] == c["id"]), None)

            # find new courses
            if not find_course and c["id"] not in [t[0] for t in new_courses]:
                new_courses.append((c["id"], c["courseName"], c["academy"], c["courseNameShort"], c["courseType"],
                                    c["beginning"], c["programmeDuration"], c["tuitionFees"], c["isElearning"],
                                    c["applicationDeadline"], c["isCompleteOnlinePossible"], c["subject"], c["link"], c["languages"]))

        # insert new courses into db
        if new_courses:
            insert_course_rows = []
            insert_language_rows = []
            for c in new_courses:
                id, course_name, academy, course_name_short, course_type, beginning, programme_duration, tuition_fees, is_elearning, application_deadline, is_complete_online_possible, subject, link, languages = c
                find_university = next(
                    (item for item in university_dicts if item["name_en"] == academy), None)
                find_university_id = find_university["id"]

                tuition_fees = tuition_fees if tuition_fees else ''
                beginning = beginning if beginning else ''
                application_deadline = application_deadline if application_deadline else ''
                programme_duration = programme_duration if programme_duration else ''

                row = (id, find_university_id, course_type, course_name, course_name_short, '', '', tuition_fees, beginning, subject, link,
                       is_elearning, application_deadline, is_complete_online_possible, programme_duration, True, datetime.now(), None)
                insert_course_rows.append(row)

                for l in languages:
                    language_id = get_language_id(l)
                    max_course_language_id += 1
                    insert_language_rows.append(
                        (max_course_language_id, id, language_id))

            statement = """INSERT INTO course 
                      (id, university_id, course_type, name_en, name_en_short, name_ch, name_ch_short, 
                      tuition_fees, beginning, subject, daadlink, is_elearning, application_deadline, 
                      is_complete_online_possible, programme_duration, is_from_daad, created_at, updated_at) 
                      VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18);"""
            await conn.executemany(statement, insert_course_rows)
            logging.info("New courses created:")
            logging.info(insert_course_rows)

            # insert course language if available
            statement = "INSERT INTO courses_languages (id, course_id, language_id) VALUES($1, $2, $3)"
            await conn.executemany(statement, insert_language_rows)
            logging.info("New courses_languages created:")
            logging.info(insert_language_rows)

        else:
            logging.info("No new course")
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logging.error(f"{e} line: {exc_tb.tb_lineno}")
    finally:
        await conn.close()

    logging.info('=========End creating new course  =========')


def get_language_id(language):
    language_id = -1
    if language == "English":
        language_id = 1
    elif language == "German":
        language_id = 2
    elif language == "Chinese":
        language_id = 3
    elif language == "French":
        language_id = 4
    elif language == "Italian":
        language_id = 5
    elif language == "Spanish":
        language_id = 6
    elif language == "Russian":
        language_id = 7
    else:
        language_id = 8
    return language_id


async def sync_course_update(daad_data, env: Environment):
    conn = await asyncpg.connect(user=env.user, password=env.password,
                                 database=env.database, host=env.host)

    logging.info('=========Start updating new course=========')
    try:
        course_rows = await conn.fetch("""SELECT c.id, c.university_id, u.name_en as university_name, c.course_type, c.name_en, c.name_en_short, c.name_ch, c.name_ch_short, 
        c.tuition_fees, c.beginning, c.subject, c.daadlink, c.is_elearning, c.application_deadline, 
        c.is_complete_online_possible, c.programme_duration 
        FROM course as c
        LEFT JOIN university as u on c.university_id = u.id
        """)
        # uni_rows = await conn.fetch('SELECT id, name_en FROM university')
        course_language_rows = await conn.fetch('SELECT id, course_id, language_id FROM courses_languages')

        # data from db
        course_dicts = [dict(row) for row in course_rows]
        # university_dicts = [dict(row) for row in uni_rows]
        course_language_dicts = [dict(row) for row in course_language_rows]

        updated_course = []

        is_any_updated = False
        for c in daad_data["courses"]:
            find_course = next(
                (item for item in course_dicts if item["id"] == c["id"]), None)

            # find udpated courses
            if find_course and (find_course["university_name"] != c["academy"] or find_course["name_en"] != c["courseName"] or
                                find_course["course_type"] != c["courseType"] or find_course["name_en_short"] != c["courseNameShort"] or
                                (find_course["tuition_fees"] and c["tuitionFees"] and find_course["tuition_fees"] != c["tuitionFees"]) or
                                (find_course["beginning"] and c["beginning"] and find_course["beginning"] != c["beginning"]) or
                                find_course["subject"] != c["subject"] or find_course["daadlink"] != c["link"] or
                                find_course["is_elearning"] != c["isElearning"] or
                                (find_course["application_deadline"] and c["applicationDeadline"] and find_course["application_deadline"] != c["applicationDeadline"]) or
                                find_course["is_complete_online_possible"] != c["isCompleteOnlinePossible"] or
                                (find_course["programme_duration"] and c["programmeDuration"]
                                and find_course["programme_duration"] != c["programmeDuration"])
                                ):
                # find_university = next((item for item in university_dicts if item["name_en"] == c["academy"]), None)
                # update columns
                id = find_course["id"]
                await update_row(conn, id, "name_en", find_course["name_en"], c["courseName"])
                await update_row(conn, id, "name_en_short", find_course["name_en_short"], c["courseNameShort"])
                await update_row(conn, id, "course_type", find_course["course_type"], c["courseType"])
                await update_row(conn, id, "tuition_fees", find_course["tuition_fees"], c["tuitionFees"])
                await update_row(conn, id, "beginning", find_course["beginning"], c["beginning"])
                await update_row(conn, id, "subject", find_course["subject"], c["subject"])
                await update_row(conn, id, "daadlink", find_course["daadlink"], c["link"])
                await update_row(conn, id, "is_elearning", find_course["is_elearning"], c["isElearning"])
                await update_row(conn, id, "application_deadline", find_course["application_deadline"], c["applicationDeadline"])
                await update_row(conn, id, "is_complete_online_possible", find_course["is_complete_online_possible"], c["isCompleteOnlinePossible"])
                await update_row(conn, id, "programme_duration", find_course["programme_duration"], c["programmeDuration"])
                is_any_updated = True

        if not is_any_updated:
            logging.info('No course updated')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logging.error(f"{e} line: {exc_tb.tb_lineno}")
    finally:
        await conn.close()

    logging.info('=========End updating new course  =========')


async def update_row(conn, id, column, source, target):
    if source != target and source and target:
        if "\'" in target:
            target = target.replace("\'", "\'\'")
        statement = f"UPDATE course SET {column} = \'{target}\', updated_at = $1 WHERE id = {id}"
        logging.info(statement)
        await conn.execute(statement, datetime.now())


async def sync_course_delete(daad_data, env: Environment):
    conn = await asyncpg.connect(user=env.user, password=env.password,
                                 database=env.database, host=env.host)

    logging.info('=========Start deleting course=========')
    try:
        course_rows = await conn.fetch("""SELECT c.id, c.university_id, u.name_en as university_name, c.course_type, c.name_en, c.name_en_short, c.name_ch, c.name_ch_short, 
        c.tuition_fees, c.beginning, c.subject, c.daadlink, c.is_elearning, c.application_deadline, 
        c.is_complete_online_possible, c.programme_duration, c.is_from_daad
        FROM course as c
        LEFT JOIN university as u on c.university_id = u.id
        """)
        # uni_rows = await conn.fetch('SELECT id, name_en FROM university')
        course_language_rows = await conn.fetch('SELECT id, course_id, language_id FROM courses_languages')

        # data from db
        course_dicts = [dict(row) for row in course_rows]

        is_any_deleted = False
        for cd in course_dicts:
            if cd["is_from_daad"] and len(str(cd["id"])) < 5:
                find_course = next(
                    (item for item in daad_data["courses"] if item["id"] == cd["id"]), None)

                if not find_course:
                    # don't delete if there's any article related to it
                    id = cd["id"]
                    article_rows = await conn.fetch(f"SELECT course_id FROM article WHERE course_id = {id}")

                    if not article_rows:
                        # delete from courses_languages
                        statement = f"DELETE FROM courses_languages WHERE course_id = {id}"
                        await conn.execute(statement)
                        logging.info(statement)

                        # dlete course
                        statement = f"DELETE FROM  course WHERE id = {id}"
                        await conn.execute(statement)
                        logging.info(statement)
                        is_any_deleted = True
                    else:
                        logging.info(
                            f"Course id = {id} is not in DAAD now, but there's a article related to it. DO NOT DELETE.")

        if not is_any_deleted:
            logging.info('No course deleted')

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logging.error(f"{e} line: {exc_tb.tb_lineno}")
    finally:
        await conn.close()

    logging.info('=========End deleting course  =========')


# data from DAAD
data = load_json(DAAD_JSON)

# set env
load_dotenv()
env = Environment(os.getenv("USER"), os.getenv("PASSWORD"),
                  os.getenv("DATABASE"), os.getenv("HOST"))

loop = asyncio.get_event_loop()

# sync university
loop.run_until_complete(sync_university(data, env))

# sync course: create, update, delete
loop.run_until_complete(sync_course_create(data, env))
loop.run_until_complete(sync_course_update(data, env))
loop.run_until_complete(sync_course_delete(data, env))


# if __name__ == "__main__":
#   sync_university()
