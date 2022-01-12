__author__ = "Gerasimos Matidis"

# This script contains the dictionaries that are used in main.py

classification_directories = ["safe", "unsafe", "burned_in"]

profiles_dictionary = {
    "cln_graphics" : "113103",
    "cln_structured_content" : "113104",
    "cln_descriptors" : "113105",
    "rtn_longitudinal_full_dates" : "113106",
    "rtn_longitudinal_modified_dates" : "113107",
    "rtn_patient_characteristics" : "113108",
    "rtn_device_id" : "113109",
    "rtn_uids" : "113110",
    "rtn_safe_private" : "113111",
    "rtn_institution_id" : "113112"
}

image_type_dictionary = {
    "burned_in" : "BurnedIn",
    "invalid_sop_class_uid" : "InvalidSOPClassUID",
    "safe" : "Safe",
    "unsafe" : "Unsafe"
}

def make_dictionary(dataset): 
    # This function creates a dictionary by extracting information 
    # from specific tags of the DICOM header (the input dataset) 
    dictionary = {
        "sop_class_uid" : dataset.SOPClassUID,
        "image_type" : "\\".join(dataset.ImageType) if hasattr(dataset, "ImageType") else None,
        "rows" : dataset.Rows if hasattr(dataset, "Rows") else None,
        "columns" : dataset.Columns if hasattr(dataset, "Columns") else None,
        "manufacturer" : dataset.Manufacturer if hasattr(dataset, "Manufacturer") else None,
        "software_versions" : dataset.SoftwareVersions if hasattr(dataset, "SoftwareVersions") else None,
        "secondary_capture_device_manufacturer" : dataset.SecondaryCaptureDeviceManufacturer 
            if hasattr(dataset, "SecondaryCaptureDeviceManufacturer") else None,
        "secondary_capture_device_manufacturer_model_name" : dataset.SecondaryCaptureDeviceManufacturerModelName
            if hasattr(dataset, "SecondaryCaptureDeviceManufacturerModelName") else None,
        "secondary_capture_device_software_versions" : dataset.SecondaryCaptureDeviceSoftwareVersions
            if hasattr(dataset, "SecondaryCaptureDeviceSoftwareVersions") else None              
    }
    return dictionary

def study_update_dictionary(study_profiles_list, query_result_data, row_index=0):
    # TODO: Add comments
    new_configuration = study_profiles_list[['cln_graphics', 'cln_structured_content', 'cln_descriptors', 
        'rtn_longitudinal_full_dates', 'rtn_longitudinal_modified_dates', 'rtn_patient_characteristics',
        'rtn_device_id', 'rtn_uids', 'rtn_safe_private', 'rtn_institution_id']].isnull().values[row_index]
    
    new_configuration = ~ new_configuration 
    if new_configuration.any():
        dictionary = {
        "cln_graphics" : study_profiles_list['cln_graphics'][row_index]
            if new_configuration[row_index] else query_result_data.cln_graphics,
        "cln_structured_content" : study_profiles_list['cln_structured_content'][row_index] 
            if new_configuration[1] else query_result_data.cln_structured_content,
        "cln_descriptors" : study_profiles_list['cln_descriptors'][row_index] 
            if new_configuration[2] else query_result_data.cln_descriptors,
        "rtn_longitudinal_full_dates" : study_profiles_list['rtn_longitudinal_full_dates'][row_index] 
            if new_configuration[3] else query_result_data.rtn_longitudinal_full_dates,
        "rtn_longitudinal_modified_dates" : study_profiles_list['rtn_longitudinal_modified_dates'][row_index] 
            if new_configuration[4] else query_result_data.rtn_longitudinal_modified_dates,
        "rtn_patient_characteristics" : study_profiles_list['rtn_patient_characteristics'][row_index] 
            if new_configuration[5] else query_result_data.rtn_patient_characteristics,
        "rtn_device_id" : study_profiles_list['rtn_device_id'][row_index] 
            if new_configuration[6] else query_result_data.rtn_device_id,
        "rtn_uids" : study_profiles_list['rtn_uids'][row_index] 
            if new_configuration[7] else query_result_data.rtn_uids,
        "rtn_safe_private" : study_profiles_list['rtn_safe_private'][row_index] 
            if new_configuration[8] else query_result_data.rtn_safe_private,
        "rtn_institution_id" : study_profiles_list['rtn_institution_id'][row_index] 
            if new_configuration[9] else query_result_data.rtn_institution_id
        }

        for key, value in dictionary.items():
            dictionary[key] = bool(value) 

    else:
        dictionary = None
    
    return dictionary