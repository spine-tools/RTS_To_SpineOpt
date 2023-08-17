from enum import IntEnum, unique
import sys
import pandas as pd
from spinedb_api import (
    DatabaseMapping,
    DiffDatabaseMapping,
    export_objects,
    export_object_parameter_values,
    export_relationships,
    export_relationship_parameter_values,
    import_relationships,
    import_relationship_classes,
    import_objects,
    import_object_parameter_values,
    import_object_parameters,
    import_relationship_parameters,
    import_relationship_parameter_values,
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


OBJECTS = export_objects(in_db)
def _extract_objects(source_object_class):
    x_to_move = []
    for value in OBJECTS:
        if value[P.CLASS] == source_object_class:
            x_to_move.append(value[P.OBJECT])
    return x_to_move


OBJECT_PARAMETER_VALUES = export_object_parameter_values(in_db)
print(OBJECT_PARAMETER_VALUES)
def _extract_object_parameter_values(parameters, source_class):
    x_to_move = []
    for value in OBJECT_PARAMETER_VALUES:
        if value[P.CLASS] == source_class and value[P.NAME] in parameters:
            x_to_move.append((value[P.OBJECT], value[P.X]))
    return x_to_move


def add_objects(object_class):
    object_list = _extract_objects(object_class)
    for obj in object_list:
        import_objects(out_db, [(object_class, obj)])


def read_translation_file(object_class):
    translation_df = pd.read_excel(translation_file, object_class)
    translation_df = translation_df.transpose().to_dict('list')
    for key in translation_df:
        translation_df[key][1] = str(translation_df[key][1])
        translation_df[key][1] = translation_df[key][1].split(", ")
        translation_df[key][2] = str(translation_df[key][2])
        translation_df[key][2] = translation_df[key][2].split(", ")
    return(translation_df)


def get_parameter_values_to_add(parameter_name,object_class):
    return(dict(_extract_object_parameter_values([parameter_name],object_class)))


def add_modified_parameter_object(base_dict,parameter_name,input_parameter_names,operation,object_class):
    aliases = {}
    for input_parameter_index in range(len(input_parameter_names)):
        input_parameter = input_parameter_names[input_parameter_index]
        aliases[input_parameter] = object_class+str(input_parameter_index)
        locals()[object_class+str(input_parameter_index)] = get_parameter_values_to_add(input_parameter,object_class)
        for i in range(len(operation)):
            operation[i] = operation[i].replace(input_parameter,object_class+str(input_parameter_index)+'_at_key')

    to_add = get_parameter_values_to_add(input_parameter_names[0],object_class)

    for key in to_add:
        for input_parameter_index in range(len(input_parameter_names)):
            locals()[object_class+str(input_parameter_index)+'_at_key'] = locals()[object_class+str(input_parameter_index)][key]
        to_add_at_key = []
        for op in operation:
            to_add_at_key.append(eval(op))
        to_add[key] = to_add_at_key

    base_dict[parameter_name]=to_add

    return(base_dict)


def create_objects(object_class):
    print("adding objects of class "+object_class)
    add_objects(object_class)

    to_add = {}
    translation_parameters = read_translation_file(object_class)

    for i in translation_parameters:
        to_add = add_modified_parameter_object(to_add,translation_parameters[i][0],translation_parameters[i][1],translation_parameters[i][2],object_class)

    parameter_names = list(to_add.keys())
    parameter_names_to_add = [tuple([object_class]+[parameter_name]) for parameter_name in parameter_names]
    import_object_parameters(out_db, parameter_names_to_add)

    parameter_values_to_add = []
    for parameter_name in parameter_names:
        for object_name in to_add[parameter_name]:
            for value_index in range(len(to_add[parameter_name][object_name])):
                value = to_add[parameter_name][object_name][value_index]
                to_append = [object_class,object_name,parameter_name,value,"alternative"+str(value_index)]
                parameter_values_to_add.append(tuple(to_append))
    import_object_parameter_values(out_db, parameter_values_to_add)

    print("added all objects of class "+object_class)


try:
    out_db.commit_session("Reload database")
finally:
    in_db.connection.close()
    out_db.connection.close()