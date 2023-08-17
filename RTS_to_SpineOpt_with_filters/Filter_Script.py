from enum import IntEnum, unique
import sys
import pandas as pd
from spinedb_api import (
    DatabaseMapping,
    DiffDatabaseMapping,
    import_objects,
    import_object_parameters,
    export_object_parameter_values,
    import_object_parameter_values
)

@unique
class P(IntEnum):
    CLASS = 0
    OBJECT = 1
    NAME = 2
    X = 3
    ALTERNATIVE = 4

in_url = sys.argv[1]
out_url = sys.argv[2]
translation_file = sys.argv[3]

in_db = DatabaseMapping(in_url)
out_db = DiffDatabaseMapping(out_url)


OBJECT_PARAMETER_VALUES = export_object_parameter_values(in_db)
#OBJECT_PARAMETERS = [(value[P.NAME],value[P.X]) for value in OBJECT_PARAMETER_VALUES]
#print(OBJECT_PARAMETERS)
def _import_objects(object_class,parameter,condition,new_name=None):
    to_import_object = []
    to_import_parameter = []
    to_import_value = []
    for value in OBJECT_PARAMETER_VALUES:
        if value[P.CLASS] == object_class and value[P.NAME]==parameter:
            try:
                """for i in range(len(OBJECT_PARAMETERS)):
                    object_parameter = OBJECT_PARAMETERS[i]
                    condition = condition.replace(object_parameter[0],'OBJECT_PARAMETERS['+str(i)+']')"""
                for value2 in OBJECT_PARAMETER_VALUES:
                    """if value2[P.OBJECT]==value[P.OBJECT] and value2[P.NAME]==parameter:
                        condition = condition.replace(value2[P.NAME],value2[P.X])"""
                condition = condition.replace(value[P.NAME],'value[P.X]')
                print(condition)
                if eval(condition):
                    if new_name is None:
                        new_name = value[P.NAME]
                    value=(value[P.CLASS],value[P.OBJECT],new_name,value[P.X],'alternative0')
                    to_import_object.append((value[P.CLASS],value[P.OBJECT]))
                    to_import_parameter.append((value[P.CLASS],new_name))
                    to_import_value.append(value)
            except:
                pass
    to_import_object=list(dict.fromkeys(to_import_object))
    to_import_parameter=list(dict.fromkeys(to_import_parameter))
    to_import_value=list(dict.fromkeys(to_import_value))

    import_objects(out_db,to_import_object)
    import_object_parameters(out_db,to_import_parameter)
    import_object_parameter_values(out_db,to_import_value)


def read_translation_file(object_class):
    translation_df = pd.read_excel(translation_file, object_class)
    translation_df.fillna('',inplace=True)
    translation_df = translation_df.transpose().to_dict('list')
    for key in translation_df:
        translation_df[key][1] = str(translation_df[key][1])
        translation_df[key][1] = translation_df[key][1].split(", ")
    return(translation_df)


def process_objects(object_class):
    print("processing objects of class "+object_class)
    lines = read_translation_file(object_class)
    for line_index in lines:
        condition_line = lines[line_index][1]
        new_name = None if lines[line_index][2] == '' else lines[line_index][2]
        for condition in condition_line:
            _import_objects(object_class,lines[line_index][0],condition,new_name)
    print("all objects of class "+object_class+" processed")


try:       
    #print(read_translation_file('unit'))
    process_objects("unit")
    out_db.commit_session("Tests")

finally:
    in_db.connection.close()
    out_db.connection.close()