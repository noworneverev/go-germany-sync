import json
import csv


def create_university_csv():
    '''
    Extract unduplicated universities from search.json
    '''
    with open('search.json', 'r', encoding='utf-8') as f:
        data = json.load(f)

    rows = []
    university = {}

    for c in data["courses"]:
        if c["academy"] not in university:
            university[c["academy"]] = c["city"]

    for key, value in university.items():
        rows.append((key, value))

    with open("university_from_searchjson.csv", "wt", encoding='utf-8', newline='') as fp:
        writer = csv.writer(fp, delimiter=",")
        # writer.writerow(["name_en", "city"])  # write header
        writer.writerows(rows)


def create_course_csv():
    '''
    Can be directly imported into table course in database
    '''
    with open('search.json', 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Read CSV file
    with open("university_data.csv", 'r', encoding='utf-8') as fp:
        reader = csv.reader(fp, delimiter=",", quotechar='"')
        next(reader, None)  # skip the headers
        universities = [row for row in reader]

    # key is university's name, value is the id in database
    universities_dict = {}

    for u in universities:
        universities_dict[u[1]] = int(u[0])

    rows = []

    for c in data["courses"]:
        u_id = universities_dict[c["academy"]]
        row = (c["id"], u_id, c["courseType"], c["courseName"], c["courseNameShort"], "", "", c["tuitionFees"], c["beginning"], c["subject"],
               c["link"], c["isElearning"], c["applicationDeadline"], c["isCompleteOnlinePossible"], c["programmeDuration"], True, "", "")
        rows.append(row)

    with open("course_data.csv", "wt", encoding='utf-8', newline='') as fp:
        writer = csv.writer(fp, delimiter=",")
        writer.writerow(["id", "university_id", "course_type", "name_en", "name_en_short", "name_ch", "name_ch_short", "tuition_fees", "beginning", "subject", "daadlink",
                        "is_elearning", "application_deadline", "is_complete_online_possible", "programme_duration", "is_from_daad", "created_at", "updated_at"])  # write header
        writer.writerows(rows)


def create_courses_languages_csv():
    '''
    Can be directly imported into table language in database
    '''
    with open('search.json', 'r', encoding='utf-8') as f:
        data = json.load(f)

    rows = []
    i = 1
    for c in data["courses"]:
        if c["languages"]:
            for l in c["languages"]:
                language_id = -1
                if l == "English":
                    language_id = 1
                elif l == "German":
                    language_id = 2
                elif l == "Chinese":
                    language_id = 3
                elif l == "French":
                    language_id = 4
                elif l == "Italian":
                    language_id = 5
                elif l == "Spanish":
                    language_id = 6
                elif l == "Russian":
                    language_id = 7
                else:
                    language_id = 8

                row = (i, c["id"], language_id)
                rows.append(row)
                i += 1

    with open("courses_languages_data.csv", "wt", encoding='utf-8', newline='') as fp:
        writer = csv.writer(fp, delimiter=",")
        writer.writerow(["language_name", "course_id", "language_id"])
        writer.writerows(rows)


# create_course_csv()
create_courses_languages_csv()
