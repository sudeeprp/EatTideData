import pandas as pd
import TideDataEater as eater
import datetime as dt
import sys
import os

academic_years = {
    "2017-18": {
        #start_date maps to the database months, which is numbered from 0
        "start_date": 20170501,
        "start_epochms": dt.datetime(2017, 6, 1).timestamp() * 1000,
        "end_date": 20180330,
        "end_epochms": dt.datetime(2018, 4, 30).timestamp() * 1000,
        "student_start_year": 2017
    },
    "2018-19": {
        "start_date": 20180920,
        "start_epochms": dt.datetime(2018, 10, 20).timestamp() * 1000,
        "end_date": 20190330,
        "end_epochms": dt.datetime(2019, 4, 30).timestamp() * 1000,
        "student_start_year": 2018
    }
}

def write_attendance(source_directory, student_demographics, academic_year):
    print("Reading attendance table")
    attendance_table = pd.read_csv(source_directory + '/attendances.csv', sep='#',
                                   usecols=['day', 'month', 'period', 'year', 'presence', 'student', 'classId'])
    attendance = attendance_table.join(student_demographics.drop\
                                           (columns=['gender',	'classes_id', 'year', 'grade_id', 'sessionEnd',
                                                     'name of grade', 'management', 'zone_number']),
                                       on='student')
    print("Total attendance entries: " + str(attendance.shape))
    attendance['timestamp'] = (attendance['year'] * 10000 +
                               attendance['month'] * 100 +
                               attendance['day']).values
    print("Writing bi_attendance_demographics.csv to " + academic_year)
    year_attendance = attendance.loc[((attendance['timestamp'] >= academic_years[academic_year]['start_date']) &
                                      (attendance['timestamp'] <= academic_years[academic_year]['end_date']))]
    year_attendance.to_csv(os.path.join(academic_year, 'bi_attendance_demographics.csv'))
    print(str(year_attendance.shape) + " written")

def write_devices(source_directory, academic_year):
    devices = eater.joiner(eater.devices_joins, source_directory, 'devices.csv')
    print("Writing devices to " + academic_year)
    devices.to_csv(os.path.join(academic_year, 'bi_devices.csv'))
    print(str(devices.shape) + " written")

def reversit(m, s):
    rev = m[s]
    m = m.drop(s, 1)
    m[s] = rev.apply(lambda x: x[::-1])
    return m

def hashcsv_writer(tabular_data, filename):
    print("Writing " + filename + '...', end='', flush=True)
    tabular_data.to_csv(filename)
    print("Done")


def printusage_and_quit(argv):
    print("Usage: " + argv[0] + " <csv source folder> <academic year>")
    print("\t<academic year> can be 2017-18 or 2018-19\n")
    sys.exit()


def parse_path(argv):
    if len(argv) != 3:
        printusage_and_quit(argv)
    return argv[1], argv[2]


source_directory, academic_year = parse_path(sys.argv)
os.makedirs(academic_year, exist_ok=True)

milestone_activities = eater.joiner(eater.milestone_event_joins, source_directory, 'milestones.csv')
print("Total milestone_activities: " + str(milestone_activities.shape))
print("Writing bi_milestone_activities.csv into " + academic_year + "...")
start_epochms = academic_years[academic_year]["start_epochms"]
end_epochms = academic_years[academic_year]["end_epochms"]
year_milestone_activities = \
    milestone_activities.loc[((milestone_activities['end'] >= start_epochms) &
                              (milestone_activities['end'] <= end_epochms) &
                              (milestone_activities['start'] >= start_epochms) &
                              (milestone_activities['start'] <= end_epochms))]
year_milestone_activities.to_csv(os.path.join(academic_year, 'bi_milestone_activities.csv'))
print(str(year_milestone_activities.shape) + " written")

student_demographics = eater.joiner(eater.student_demographic_joins, source_directory, 'students.csv').rename\
    (columns={'cluster': 'cluster_id', 'block': 'mandal_id', 'district': 'division_id', 'zone': 'district_id'})
print("Total student_demographics: " + str(student_demographics.shape))
year_student_demographics = student_demographics.\
    loc[student_demographics['year']==academic_years[academic_year]["student_start_year"]]

student_duplicate_markers = year_student_demographics.index.duplicated(keep='last')
duplicate_students = year_student_demographics[student_duplicate_markers]
if(len(duplicate_students) > 0):
    print("\nWarning! Duplicate student IDs found in the academic year. \nNumber of duplicates: " +
          str(len(duplicate_students)))

year_student_demographics = year_student_demographics[~student_duplicate_markers]
print("Writing bi_student_demographics.csv into " + academic_year + "...")
year_student_demographics.to_csv(os.path.join(academic_year, 'bi_student_demographics.csv'), index_label = 'student_id')
print(str(year_student_demographics.shape) + " written")

write_attendance(source_directory, year_student_demographics, academic_year)

write_devices(source_directory, academic_year)
