import pandas as pd
import TideDataEater as eater
import datetime as dt
import sys
import os

academic_years = {
    "2017-18": {
        "start_date": 20170501,
        "start_epochms": dt.datetime(2017, 6, 1).timestamp() * 1000,
        "end_date": 20180331,
        "end_epochms": dt.datetime(2018, 3, 31).timestamp() * 1000,
        "student_start_year": 2017
    },
    "2018-19": {
        "start_date": 20180501,
        "start_epochms": dt.datetime(2018, 6, 1).timestamp() * 1000,
        "end_date": 20190331,
        "end_epochms": dt.datetime(2019, 3, 31).timestamp() * 1000,
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
    attendance['timestamp'] = (attendance['year'] * 10000 +\
                               attendance['month'] * 100 +\
                               attendance['day']).values
    print("Writing attendance_demographics.csv to " + academic_year)
    year_attendance = attendance.loc[attendance['timestamp'] >= academic_years[academic_year]['start_date']]
    #TODO: Put this back after solving the 'duplicate' problem
    #Otherwise change the condition (to <=) in order to get previous years data
    #year_attendance = year_attendance.loc[attendance['timestamp'] <= academic_years[academic_year]['end_date']]
    year_attendance.to_csv(os.path.join(academic_year, 'attendance_demographics.csv'))
    print(str(year_attendance.shape) + " written")

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
year_milestone_activities = \
    milestone_activities.loc[milestone_activities['end']>=academic_years[academic_year]["start_epochms"]].\
        loc[milestone_activities['end']<=academic_years[academic_year]["end_epochms"]]
year_milestone_activities.to_csv(os.path.join(academic_year, 'bi_milestone_activities.csv'))
print(str(year_milestone_activities.shape) + " written")
'''
academic_end = dt.datetime(2018, 5, 31)
print("Writing bi_milestone_activities.csv up until " + str(academic_end.date()))
milestone_activities.loc\
    [milestone_activities['end']<=academic_end.timestamp()*1000].to_csv('bi_milestone_activities.csv')
'''

student_demographics = eater.joiner(eater.student_demographic_joins, source_directory, 'students.csv')
print("Total student_demographics: " + str(student_demographics.shape))
print("Writing bi_student_demographics.csv into " + academic_year + "...")
year_student_demographics = student_demographics.rename\
    (columns={'cluster': 'cluster_id', 'block': 'mandal_id', 'district': 'division_id', 'zone': 'district_id'}).\
    loc[student_demographics['year']==academic_years[academic_year]["student_start_year"]]
year_student_demographics.to_csv(os.path.join(academic_year, 'bi_student_demographics.csv'), index_label = 'student_id')
print(str(year_student_demographics.shape) + " written")

write_attendance(source_directory, student_demographics, academic_year)
