import pandas as pd
import os
import datetime
import time

student_demographic_joins = {'students.csv': {'index_col': 'id',
                                              'usecols': ['id', 'gender'],
                                              'col_rename': None,
                                              'join_with': 'classes_students.csv', 'join_on': None},
                             'classes_students.csv': {'index_col': 'students_id',
                                                      'usecols': ['classes_id', 'students_id'],
                                                      'col_rename': None,
                                                      'join_with': 'classes.csv', 'join_on': 'classes_id'},
                             'classes.csv': {'index_col': 'id',
                                             'usecols': ['id', 'year', 'grade_id', 'sessionEnd'],
                                             'col_rename': None,
                                             'join_with': 'grades.csv', 'join_on': 'grade_id'},
                             'grades.csv': {'index_col': 'id',
                                            'usecols': ['id', 'name', 'school_id'],
                                            'col_rename': {'name': 'name of grade'},
                                            'join_with': 'schools.csv', 'join_on': 'school_id'},
                             'schools.csv': {'index_col': 'id',
                                             'usecols': ['id', 'management', 'name', 'cluster'],
                                             'resolve_duplicates': 'name',
                                             'col_rename': {'name': 'name of school'},
                                             'join_with': 'clusters.csv', 'join_on': 'cluster'},
                             'clusters.csv': {'index_col': 'id',
                                              'usecols': ['id', 'name', 'block'],
                                              'resolve_duplicates': 'name',
                                              'col_rename': {'name': 'name of cluster'},
                                              'join_with': 'blocks.csv', 'join_on': 'block'},
                             'blocks.csv': {'index_col': 'id',
                                            'usecols': ['id', 'name', 'district'],
                                            'resolve_duplicates': 'name',
                                            'col_rename': {'name': 'name of mandal'},
                                            'join_with': 'districts.csv', 'join_on': 'district'},
                             'districts.csv': {'index_col': 'id',
                                               'usecols': ['id', 'name', 'zone'],
                                               'resolve_duplicates': 'name',
                                               'col_rename': {'name': 'name of division'},
                                               'join_with': 'zones.csv', 'join_on': 'zone'},
                             'zones.csv': {'index_col': 'id',
                                           'usecols': ['id', 'name', 'state'],
                                           'resolve_duplicates': 'name',
                                           'col_rename': {'name': 'name of district', 'state': 'zone_number'},
                                           'join_with': None, 'join_on': None}
                             }

milestone_event_joins = {'milestones.csv': {'index_col': None,
                                            'usecols': ['id', 'end', 'milestoneId', 'start', 'status', 'ladder_id'],
                                            'col_rename': None,
                                            'join_with': 'ladders.csv', 'join_on': 'ladder_id'},
                         'ladders.csv': {'index_col': 'id',
                                         'usecols': ['id', 'classId', 'courseId', 'studentId'],
                                         'col_rename': None,
                                         'join_with': 'courses.csv', 'join_on': 'courseId'},
                         'courses.csv': {'index_col': 'id',
                                         'usecols': ['id', 'name', 'grade_id'],
                                         'col_rename': {'name': 'nameofcourse'},
                                         'join_with': 'grades.csv', 'join_on': 'grade_id'},
                         'grades.csv': {'index_col': 'id',
                                        'usecols': ['id', 'name', 'school_id'],
                                        'col_rename': {'name': 'nameofgrade'},
                                        'join_with': 'schools.csv', 'join_on': 'school_id'},
                         'schools.csv': {'index_col': 'id',
                                         'usecols': ['id', 'management', 'name', 'cluster'],
                                         'resolve_duplicates': 'name',
                                         'col_rename': {'name': 'name of school'},
                                         'join_with': 'clusters.csv', 'join_on': 'cluster'},
                         'clusters.csv': {'index_col': 'id',
                                          'usecols': ['id', 'name', 'block'],
                                          'resolve_duplicates': 'name',
                                          'col_rename': {'name': 'name of cluster'},
                                          'join_with': 'blocks.csv', 'join_on': 'block'},
                         'blocks.csv': {'index_col': 'id',
                                        'usecols': ['id', 'name', 'district'],
                                        'resolve_duplicates': 'name',
                                        'col_rename': {'name': 'name of mandal'},
                                        'join_with': 'districts.csv', 'join_on': 'district'},
                         'districts.csv': {'index_col': 'id',
                                           'usecols': ['id', 'name', 'zone'],
                                           'resolve_duplicates': 'name',
                                           'col_rename': {'name': 'name of division'},
                                           'join_with': 'zones.csv', 'join_on': 'zone'},
                         'zones.csv': {'index_col': 'id',
                                       'usecols': ['id', 'name', 'state'],
                                       'resolve_duplicates': 'name',
                                       'col_rename': {'name': 'name of district', 'state': 'zone_number'},
                                       'join_with': None, 'join_on': None}
                         }

devices_joins = { 'devices.csv': {'index_col': 'serial',
                                  'usecols': None,
                                  'col_rename': None,
                                  'join_with': 'schools.csv', 'join_on': 'school'},
                  'schools.csv': {'index_col': 'id',
                                  'usecols': ['id', 'management', 'name', 'cluster'],
                                  'resolve_duplicates': 'name',
                                  'col_rename': {'name': 'name of school'},
                                  'join_with': 'clusters.csv', 'join_on': 'cluster'},
                  'clusters.csv': {'index_col': 'id',
                                   'usecols': ['id', 'name', 'block'],
                                   'resolve_duplicates': 'name',
                                   'col_rename': {'name': 'name of cluster'},
                                   'join_with': 'blocks.csv', 'join_on': 'block'},
                  'blocks.csv': {'index_col': 'id',
                                 'usecols': ['id', 'name', 'district'],
                                 'resolve_duplicates': 'name',
                                 'col_rename': {'name': 'name of mandal'},
                                 'join_with': 'districts.csv', 'join_on': 'district'},
                  'districts.csv': {'index_col': 'id',
                                    'usecols': ['id', 'name', 'zone'],
                                    'resolve_duplicates': 'name',
                                    'col_rename': {'name': 'name of division'},
                                    'join_with': 'zones.csv', 'join_on': 'zone'},
                  'zones.csv': {'index_col': 'id',
                                'usecols': ['id', 'name', 'state'],
                                'resolve_duplicates': 'name',
                                'col_rename': {'name': 'name of district', 'state': 'zone_number'},
                                'join_with': None, 'join_on': None}

                  }

def read_table_from_file(joins, directory, filename):
    next_table = pd.read_csv(directory + '/' + filename, sep='#', index_col=joins[filename]['index_col'],
                             usecols=joins[filename]['usecols'])

    if('resolve_duplicates' in joins[filename].keys()):
        field_to_resolve = joins[filename]['resolve_duplicates']
        duplicates = next_table.loc[next_table.duplicated(subset=field_to_resolve, keep=False)]
        for index, row in duplicates.iterrows():
            next_table.loc[index, field_to_resolve ] = row[field_to_resolve ] + '-' + str(index)

    columns_to_rename = joins[filename]['col_rename']
    if columns_to_rename is not None:
        next_table.rename(columns=columns_to_rename, inplace=True)
    return next_table


def next_join(joins, filename):
    return joins[filename]['join_with'], joins[filename]['join_on']


def joiner(joins, directory, start_file):
    accumulator = read_table_from_file(joins, directory, start_file)
    next_file, next_join_on = next_join(joins, start_file)
    while next_file is not None:
        print('Joining ' + next_file)
        next_table = read_table_from_file(joins, directory, next_file)
        accumulator = accumulator.join(next_table, on=next_join_on)
        next_file, next_join_on = next_join(joins, next_file)
    return accumulator


def format_time_as_date(usual_time):
    return usual_time.strftime('%d-%b-%Y')


def epochms_to_time(epoch_time):
    return datetime.datetime.fromtimestamp(round(epoch_time / 1000))


def format_epochms_as_date(epoch_time):
    return format_time_as_date(epochms_to_time(epoch_time))


def read_student_demographics(dir, student_file):
    print('Reading students')
    students = read_table_from_file(dir, student_file)
    return students

