__author__ = "Gerasimos Matidis"

# This script contains all the main functions of MIES

import os
import re
import argparse
import configparser
from radiodb import *
from radiodb import Session
from radiodb import DataBaseHandler
from pacsprocs import *
import pandas as pd
import csv
from time import sleep, time
from datetime import datetime, timedelta
from sqlalchemy import select, update, delete, or_, and_, func
import logging
import logging.config
import pydicom
from pydicom.dataelem import DataElement
import shutil
from distutils.dir_util import copy_tree
import tempfile
from dicts import classification_directories as label_dirs
from dicts import profiles_dictionary as prof_dict 
from dicts import image_type_dictionary as im_type_dict
from dicts import make_dictionary
from dicts import study_update_dictionary

def create_new_research_study_auto():

    # search within the source directory for "xls" files
    for dirpath, dirname, filenames in os.walk(args.source):
        for f in filenames:
            if f.endswith('.xlsx'):
                file_path = os.path.join(dirpath, f)

                # read the "xls" file
                study_list = pd.read_excel(file_path)

                # create a "datetime-style" name and rename the "xls" file in order to be archived at the end 
                new_f = f'{datetime.now().strftime("%Y%m%d%H%M%S")}.xlsx'
                new_file_path = os.path.join(dirpath, new_f)
                os.rename(file_path, new_file_path)
                
                for i in study_list.index: 
                    sleep(1)  
                    print(i, f)
                    # create a boolean variable to indicate if the 2 options for Retaining 
                    # Longitudinal Temporal Information were given simultaneously
                    invalid_date_profiles = (study_list['rtn_longitudinal_full_dates'][i] == 1
                        and study_list['rtn_longitudinal_modified_dates'][i] == 1)

                    # create a boolean variable to indicate if an invalid email format was given
                    invalid_email = re.match(r"[^@ ,]+@[^@ ,]+\.[^@ ,]",study_list['researchers'][i]) is None
                    
                    if invalid_date_profiles or invalid_email:
                        if invalid_date_profiles:
                            logging.error(f"{new_f}: (study_acronym = {study_list['study_acronym'][i]}) The 2 options for "
                                f"Retaining Longitudinal Temporal Information are MUTUALLY EXCLUSIVE. Either the option with "
                                "the full dates or that with the modified ones can be chosen.")
                        else:
                            logging.error(f"{new_f}: (study_acronym = {study_list['study_acronym'][i]}) Invalid email format "
                            "is given.")
                    else:
                        # query to check whether the study already exists 
                        q = session.query(ResearchStudyInfo).filter(
                            ResearchStudyInfo.study_acronym == study_list['study_acronym'][i])
                        result = q.one_or_none()

                        if result is None:

                            # from the i-th line of the "xls" file extract a dictionary with the needed info 
                            conf_list=study_list.iloc[i, 2:12].fillna(0)
                            first_conf_dict = conf_list.to_dict()

                            # create a concatenated string that represents the profile configuration (e.g. 113103/113104/113105)
                            profile_code = ""
                            for key, value in first_conf_dict.items():
                                first_conf_dict[key] = bool(value)
                                if first_conf_dict[key] is True:
                                    profile_code += prof_dict[key] + "/"
                            
                            # create and add the record
                            rec = ResearchStudyInfo(
                                study_acronym = study_list['study_acronym'][i], 
                                researchers = study_list['researchers'][i],
                                cln_graphics = first_conf_dict['cln_graphics'],
                                cln_structured_content = first_conf_dict['cln_structured_content'],
                                cln_descriptors = first_conf_dict['cln_descriptors'],
                                rtn_longitudinal_full_dates = first_conf_dict['rtn_longitudinal_full_dates'],
                                rtn_longitudinal_modified_dates = first_conf_dict['rtn_longitudinal_modified_dates'],
                                rtn_patient_characteristics = first_conf_dict['rtn_patient_characteristics'],
                                rtn_device_id = first_conf_dict['rtn_device_id'],
                                rtn_uids = first_conf_dict['rtn_uids'],
                                rtn_safe_private = first_conf_dict['rtn_safe_private'],
                                rtn_institution_id = first_conf_dict['rtn_institution_id'])
                            session.add(rec)
                            logging.info(f'{new_f}: A new study has been submitted (study_acronym: "{study_list["study_acronym"][i]}", '
                                f'profile: "{profile_code}").')
     
                        else: 
                            
                            # check whether a new profile coniguration is defined and, if so, update the "ResearchStudyInfo" table
                            new_conf_dict = study_update_dictionary(study_list, result, i)
                            
                            if new_conf_dict is not None:
                                if not (new_conf_dict["rtn_longitudinal_full_dates"] and new_conf_dict["rtn_longitudinal_modified_dates"]):
                                    if not pd.isnull(study_list["researchers"][i]):
                                        researchers = study_list["researchers"][i]
                                    else:
                                        researchers = result.researchers
                                    stmt = update(ResearchStudyInfo).\
                                        where(ResearchStudyInfo.study_acronym == study_list['study_acronym'][i]).\
                                        values(
                                            researchers = researchers,
                                            cln_graphics = new_conf_dict["cln_graphics"],
                                            cln_structured_content = new_conf_dict["cln_structured_content"],
                                            cln_descriptors = new_conf_dict["cln_descriptors"],
                                            rtn_longitudinal_full_dates = new_conf_dict["rtn_longitudinal_full_dates"],
                                            rtn_longitudinal_modified_dates = new_conf_dict["rtn_longitudinal_modified_dates"],
                                            rtn_patient_characteristics = new_conf_dict["rtn_patient_characteristics"],
                                            rtn_device_id = new_conf_dict["rtn_device_id"],
                                            rtn_uids = new_conf_dict["rtn_uids"],
                                            rtn_safe_private = new_conf_dict["rtn_safe_private"],
                                            rtn_institution_id = new_conf_dict["rtn_institution_id"])
                                    session.execute(stmt)
                                    logging.info(f"{new_f}: (study_acronym = {study_list['study_acronym'][i]}) The Research study has been updated.")
                                else:
                                    logging.error(f'{new_f}: (study_acronym = {result.study_acronym}) The 2 options for Retaining Longitudinal Temporal Information are '
                                        'MUTUALLY EXCLUSIVE. They cannot be both chosen (True).')
                
                # archive the "xls" file 
                shutil.move(new_file_path, args.archive) 
                session.commit()

    return None

def create_new_research_study_manual():

    # check if the 2 options for Retaining Longitudinal Temporal Information were given simultaneously and raise an error
    if args.rtn_longitudinal_full_dates and args.rtn_longitudinal_modified_dates is True:
        msg1 = (f"(study_acronym: {args.study_acronym}) The 2 options for Retaining Longitudinal Temporal Information "
        "are MUTUALLY EXCLUSIVE. \nEither the option with the full dates or that with the modified ones can be chosen.")
        logging.error(msg1)
        raise ValueError(msg1)
    
    # check if an invalid email format was given and raise an error
    if not re.match(r"[^@ ,]+@[^@ ,]+\.[^@ ,]", args.email_address):
        msg2 = f"(study_acronym: {args.study_acronym}) Invalid email format is given."
        logging.error(msg2)
        raise SyntaxError(msg2) 
    
    # create a concatenated string that represents the profile configuration (e.g. 113103/113104/113105)
    profile_code = ""
    args_dict = args.__dict__
    args_dict['cln_graphics'] = args_dict.pop('no_cln_graphics')
    args_dict['cln_structured_content'] = args_dict.pop('no_cln_structured_content')
    args_dict['cln_descriptors'] = args_dict.pop('no_cln_descriptors')
    for profile in prof_dict:
        if args_dict[profile] is True:
            profile_code += prof_dict[profile] +"-"
    
    # query to check whether the study already exists
    q = session.query(ResearchStudyInfo.study_acronym).filter(
        ResearchStudyInfo.study_acronym == args.study_acronym)
    result = q.one_or_none()   
    
    if result is None:
        rec = ResearchStudyInfo(
            study_acronym=args.study_acronym, 
            researchers=args.email_address,
            cln_graphics=args.cln_graphics,
            cln_structured_content=args.cln_structured_content,
            cln_descriptors=args.cln_descriptors,
            rtn_longitudinal_full_dates=args.rtn_longitudinal_full_dates,
            rtn_longitudinal_modified_dates=args.rtn_longitudinal_modified_dates,
            rtn_patient_characteristics=args.rtn_patient_characteristics,
            rtn_device_id=args.rtn_device_id,
            rtn_uids=args.rtn_uids,
            rtn_safe_private=args.rtn_safe_private,
            rtn_institution_id=args.rtn_institution_id)
        session.add(rec)

        logging.info(f'A new study has been submitted (study_acronym: {args.study_acronym}, researcher: {args.email_address}, '
        f'profile: {profile_code}).')

    else: 
        stmt = update(ResearchStudyInfo).\
            where(ResearchStudyInfo.study_acronym == args.study_acronym).\
            values(
                researchers=args.email_address,
                cln_graphics=args.cln_graphics,
                cln_structured_content=args.cln_structured_content,
                cln_descriptors=args.cln_descriptors,
                rtn_longitudinal_full_dates=args.rtn_longitudinal_full_dates,
                rtn_longitudinal_modified_dates=args.rtn_longitudinal_modified_dates,
                rtn_patient_characteristics=args.rtn_patient_characteristics,
                rtn_device_id=args.rtn_device_id,
                rtn_uids=args.rtn_uids,
                rtn_safe_private=args.rtn_safe_private,
                rtn_institution_id=args.rtn_institution_id)
                
        session.execute(stmt)
        logging.info(f'(study_acronym: {args.study_acronym}) The Research study has been updated (researcher: {args.email_address}, '
        f'profile: {profile_code}).')
    
    session.commit()
    return None

def create_new_job():

    # search within the source directory for "xls" files
    for dirpath, dirname, filenames in os.walk(args.source):
        for f in filenames:
            if f.endswith('.xlsx'):
                sleep(1)
                file_path = os.path.join(dirpath, f)

                # read the "xls" file
                job_list = pd.read_excel(file_path, sheet_name='JOB')
                study_list = pd.read_excel(file_path, sheet_name='STUDY')

                # create a "datetime-style" name and rename the "xls" file in order to be archived at the end
                new_f = f'{datetime.now().strftime("%Y%m%d%H%M%S")}.xlsx'
                new_file_path = os.path.join(dirpath, new_f)
                os.rename(file_path, new_file_path)

                # check if exactly 1 study acronym and maximum 1 profile configuration were given
                if len(study_list) != 1:  
                    logging.error(f"{new_f}: Expected exactly 1 study_acronym and "
                    f"(optionally) 1 profile configuration, {len(study_list)} were given.")   

                else:
                    # the study acronym is missing from the "xls" file
                    if pd.isnull(study_list['study_acronym'][0]): 
                        logging.error(f"{new_f}: The study_acronym where the jobs refer to "
                        "is not determined within the file.")
                    else: 
                        # query to check whether the study that the job refers to already exists in the DB
                        q = session.query(ResearchStudyInfo).filter(
                            ResearchStudyInfo.study_acronym == study_list['study_acronym'][0])
                        result = q.one_or_none() 

                        if result is not None:
                            new_conf_dict = study_update_dictionary(study_list, result)

                            # If a new profile configuration was given, update the recordf in DB
                            if new_conf_dict is not None: 
                                if not (new_conf_dict["rtn_longitudinal_full_dates"] and new_conf_dict["rtn_longitudinal_modified_dates"]):
                                    stmt = update(ResearchStudyInfo).\
                                        where(ResearchStudyInfo.study_acronym == study_list['study_acronym'][0]).\
                                        values(cln_graphics = new_conf_dict["cln_graphics"],
                                            cln_structured_content = new_conf_dict["cln_structured_content"],
                                            cln_descriptors = new_conf_dict["cln_descriptors"],
                                            rtn_longitudinal_full_dates = new_conf_dict["rtn_longitudinal_full_dates"],
                                            rtn_longitudinal_modified_dates = new_conf_dict["rtn_longitudinal_modified_dates"],
                                            rtn_patient_characteristics = new_conf_dict["rtn_patient_characteristics"],
                                            rtn_device_id = new_conf_dict["rtn_device_id"],
                                            rtn_uids = new_conf_dict["rtn_uids"],
                                            rtn_safe_private = new_conf_dict["rtn_safe_private"],
                                            rtn_institution_id = new_conf_dict["rtn_institution_id"])
                                    session.execute(stmt)
                                    logging.info(f"{new_f}: A new profile configuration has been applied to the study_acronym '{result.study_acronym}'")
                                else:
                                    logging.error(f'{new_f}: (study_acronym = {result.study_acronym}) The 2 options for Retaining Longitudinal Temporal Information are '
                                        'MUTUALLY EXCLUSIVE. They cannot be both chosen (True).') 
                            job = ImageExportJob(study_id = result.study_id)    
                            records_list = []
                            check_list = []
                            # for every DICOM study in the "xls" file
                            for i in job_list.index:
                                pat_id = job_list['MDN'][i]
                                
                                if type(pat_id) == str: # TODO nato diorthoso
                                    pat_id = pat_id.strip()
                                    pat_id = eval(pat_id)

                                # if needed, set the patient ID to the proper format (7-digit)
                                if pat_id < 1000000:
                                    pat_id = format(pat_id, '07d')

                                accnr = job_list['accnr'][i]
                                if type(accnr) == str:
                                    accnr = accnr.strip()

                                pair = [pat_id, accnr]

                                if pair not in check_list:

                                    check_list.append(pair)

                                    # query to check whether this DICOM study has already been submitted by an older job
                                    q2 = (session.query(ResearchStudyInfo, StudyExportRecord)
                                        .filter(ResearchStudyInfo.study_id == ImageExportJob.study_id)
                                        .filter(ImageExportJob.job_id == StudyExportRecord.job_id)
                                        .filter(
                                            and_(ResearchStudyInfo.study_acronym == result.study_acronym,
                                                StudyExportRecord.patient_id == pat_id, 
                                                StudyExportRecord.accession_number == accnr)))
                                    result_2 = q2.one_or_none()

                                    if result_2 is None:

                                        # add a new study record for this DICOM study and update its patient ID, 
                                        # accession number and pseudo ID according to the info from "xls" file
                                        rec = StudyExportRecord(patient_id = pat_id, accession_number = job_list['accnr'][i],
                                            pseudo_id = job_list['pseudo'][i])
                                        records_list.append(rec)
                                    else:
                                        logging.info(f'The study record with ID = {result_2.StudyExportRecord.record_id} was skipped, because '
                                        f'it has already been submitted for the study_acronym "{result_2.ResearchStudyInfo.study_acronym}" '
                                        f'by the Job with ID = {result_2.StudyExportRecord.job_id}.')
                                    
                                job.StudyExportRecord = records_list
                                   
                            session.add(job)
                            session.commit()
                            
                            # query to update the "StudyExportJob" table with the number of the studies of each job
                            q3 = session.query(func.count(StudyExportRecord.record_id)).filter(
                                StudyExportRecord.job_id == job.job_id).scalar()

                            stmt_2 = update(ImageExportJob).\
                                where(ImageExportJob.job_id == job.job_id).\
                                values(total_studies=q3)
                            
                            session.execute(stmt_2)

                        else: 
                            logging.error(f"{new_f}: (study_acronym = {study_list['study_acronym'][0]}) The study_acronym where the job "
                            "refers to does not exist in the DataBase. It first has to be created.")
                
                shutil.move(new_file_path, args.archive)
                session.commit()
    
    return None

def find_session():

    timelimit = args.timelimit
    failures_limit = args.failures_limit

    time_thresh = datetime.now() - timedelta(hours=timelimit)

    # query to select the records that are in "NotStarted" state, as well as the records that are in "PacsFindFailed" state 
    # and their last modification was before the time that is defined by the "time_thresh" 
    q = session.query(StudyExportRecord).filter(
        or_(StudyExportRecord.state == StudyExportState.NotStarted, 
        and_(StudyExportRecord.state == StudyExportState.PacsFindFailed, StudyExportRecord.modified < time_thresh)))
    
    for i in q: 
        # query to the PACS (C-FIND) to search for a DICOM study with the patient ID and the accession number 
        # that are refered in the current study record in the DB. (below "data_set" is the DICOM header)
        data_set, matches, msg = pacs_find(args.hostname, args.port, (args.host_ae_title).encode('ASCII'), 
            (args.user_ae_title).encode('ASCII'), args.query_retrieve_level, i.accession_number, i.patient_id)
        
        failures = i.number_of_failures + 1
        if matches is not None:
            if matches == 1:
                # check whether the value of ModalitiesInStudy in the header is a list (i.e. multiple modalities).
                # If so, convert the list items to a concatenated string 
                if isinstance(data_set.ModalitiesInStudy, str):
                    modals = data_set.ModalitiesInStudy
                else:
                    modals = "\\".join(data_set.ModalitiesInStudy)

                # check whether the attribute "StationName" exists in the header or not
                if hasattr(data_set, 'StationName'):
                    station = data_set.StationName
                else:
                    station = None

                stmt = update(StudyExportRecord).\
                    where(StudyExportRecord.record_id == i.record_id).\
                    values(patient_birth_date = data_set.PatientBirthDate, 
                        study_description = data_set.StudyDescription, 
                        study_instance_uid = data_set.StudyInstanceUID,
                        study_date = data_set.StudyDate,
                        study_time = data_set.StudyTime,
                        state = StudyExportState.PacsFindDone,          
                        description = msg,
                        number_of_failures = 0,
                        modified = datetime.now(),
                        images_in_pacs = data_set.NumberOfStudyRelatedInstances,
                        modalities_in_study = modals,
                        station_name = station)
            else:
                # there was not found any DICOM study in PACS and the record's state is set to "Failed"
                logging.error(f"(PatientID: {i.patient_id}, AccessionNumber: {i.accession_number}) {msg}")
                stmt = update(StudyExportRecord).\
                    where(StudyExportRecord.record_id == i.record_id).\
                    values(state = StudyExportState.Failed, 
                        modified = datetime.now(),
                        description = msg,
                        number_of_failures = failures)
                
        else:
            # the connection with the PACS was not possible
            if failures == failures_limit:
                new_state = StudyExportState.Failed  
                logging.error(f"(hostname: {args.hostname}, host_AE_title: {args.host_ae_title}, "
                    f"user_AE_title: { args.user_ae_title}) {msg}")   
            else:
                new_state = StudyExportState.PacsFindFailed
                msg = f"{msg} (first attempt)"
                logging.warning(f"(hostname: {args.hostname}, host_AE_title: {args.host_ae_title}, "
                    f"user_AE_title: { args.user_ae_title}) {msg}")
                
            stmt = update(StudyExportRecord).\
                where(StudyExportRecord.record_id == i.record_id).\
                values(state = new_state, 
                    modified = datetime.now(),
                    description = msg,
                    number_of_failures = failures)
        
        session.execute(stmt)
        session.commit() 

    return None   

def move_session():

    timelimit = args.timelimit
    failures_limit = args.failures_limit
    output_path = args.output_path
    output_studies = os.listdir(output_path)

    # query to check whether the studies in "PacsMoveFailed" state have eventually been stored into the "cache" folder.
    # NOTE: The reason is that sometimes PACS sends a negative response in a C-MOVE request although it moves the files.
    q0 = session.query(StudyExportRecord.record_id, StudyExportRecord.study_instance_uid).filter(
        StudyExportRecord.state == StudyExportState.PacsMoveFailed)
    
    for h in q0:
        if h.study_instance_uid in [item.split('-')[0] for item in output_studies]:
            stmt0 = update(StudyExportRecord).\
                where(StudyExportRecord.record_id == h.record_id).\
                values(state=StudyExportState.PacsMoveDone, modified=datetime.now())
            session.execute(stmt0)
            session.commit()    

    time_thresh = datetime.now() - timedelta(hours=timelimit)

    # query to select the first 10 records that are in "PacsFindDone" state, as well as the records that are in 
    # "PacsMoveFailed" state and their last modification was before the time that is defined by the "time_thresh" 
    q1 = session.query(StudyExportRecord).filter(or_(StudyExportRecord.state == StudyExportState.PacsFindDone, 
        and_(StudyExportRecord.state == StudyExportState.PacsMoveFailed, StudyExportRecord.modified < time_thresh))).limit(5)
    
    for i in q1:
        
        # query to select the records that are in an "active" state (i.e. their files are still in "dcmhub" server)
        q2 = session.query(StudyExportRecord).filter(
            or_(StudyExportRecord.state == StudyExportState.PacsMoveDone,
                StudyExportRecord.state == StudyExportState.PacsReceiveIncomplete, 
                StudyExportRecord.state == StudyExportState.ExportedToCTP))

        active_num = len(q2.all()) 
        
        # If the limit of the active studies is reached, then the rest C-MOVE requests are skipped 
        if active_num > (args.active_jobs_limit - 1): 
            break

        # query to check whether there is any other study record that has the same Study Instance UID and it is currently in 
        # one of the included states. If not, the C-MOVE operation is applied, otherwise the current record is skipped for now       
        q3 = session.query(StudyExportRecord).filter(
            and_(StudyExportRecord.study_instance_uid == i.study_instance_uid,
                StudyExportRecord.record_id != i.record_id,
                or_(StudyExportRecord.state == StudyExportState.PacsMoveFailed,
                    StudyExportRecord.state == StudyExportState.PacsMoveDone,
                    StudyExportRecord.state == StudyExportState.PacsReceiveIncomplete, 
                    StudyExportRecord.state == StudyExportState.ExportedToCTP)))
        result = q3.all()
        
        if not result:  
            # Apply the C-MOVE service operation using the Study Instance UID
            msg = pacs_move(args.hostname, args.port, (args.host_ae_title).encode('ASCII'), (args.user_ae_title).encode('ASCII'), 
                (args.receiver_ae_title).encode('ASCII'), args.query_retrieve_level, i.study_instance_uid)
            
            if not msg: # the message is empty, i.e. the C-MOVE operation was succeed
                new_state = StudyExportState.PacsMoveDone
                failures = 0
                msg = "The C-MOVE operation was succeed."
                logging.info(f'(Study Instance UID: {i.study_instance_uid}) {msg}')
            else:
                failures = i.number_of_failures + 1
                if failures == failures_limit:
                    new_state = StudyExportState.Failed
                    logging.error(f"(Study Instance UID: {i.study_instance_uid}) {msg}")
                else:
                    new_state = StudyExportState.PacsMoveFailed
                    logging.warning(f"(Study Instance UID: {i.study_instance_uid}) {msg}")
                    
            stmt = update(StudyExportRecord).\
            where(StudyExportRecord.record_id == i.record_id).\
            values(state = new_state,
                description = msg, 
                number_of_failures = failures,
                modified = datetime.now()) 
            session.execute(stmt)   
        else:
            msg = f"The record with ID={i.record_id} was skipped."
            logging.info(msg)
        
        session.commit()     
 
    return None

def prepare_session():

    timelimit = args.timelimit
    failures_limit = args.failures_limit

    time_thresh = datetime.now() - timedelta(hours=timelimit)

    # define the source directory and list its 1st level contents
    source_path = args.source
    dirs = os.listdir(source_path)

    for item in dirs:
        # regular expression to check if the item has a pre-defined name format (the parentheses in the first
        # argument separate the match in 2 parts, one before the dash symbol and one after)
        m = re.match(r"^([0-9][0-9\.]+)-([^~]*)$", item)
        if m:
            st_uid = m.group(1)# 1 is the first part of the match

            # query to select if there are records which have Study Insance UID = "st_uid" and either they are in "PacsMOveDone" state
            # or they are in "PacsReceiveIncomplete" state and their last modification was before the time that is defined by the "time_thresh"
            q1 = (session.query(ResearchStudyInfo, StudyExportRecord)
                .filter(ResearchStudyInfo.study_id == ImageExportJob.study_id)
                .filter(ImageExportJob.job_id == StudyExportRecord.job_id)
                .filter(and_(StudyExportRecord.study_instance_uid == st_uid, 
                    or_(StudyExportRecord.state == StudyExportState.PacsMoveDone, 
                        StudyExportRecord.state == StudyExportState.PacsMoveFailed,
                        StudyExportRecord.state == StudyExportState.ExportedToCTP,
                        StudyExportRecord.state == StudyExportState.Incomplete,
                        and_(StudyExportRecord.state == StudyExportState.PacsReceiveIncomplete, StudyExportRecord.modified < time_thresh)))))

            result = q1.one_or_none() 
            if result is not None:

                # by using the profile dictionary check which profiles are enabled for the current research study and create 
                # a concatenated string from their codes (e.g the basic profile would be "113103/113104/113105")
                profile = ""
                for x, y in prof_dict.items():
                    if getattr(result.ResearchStudyInfo, x) is True:
                        profile += (y + "/") 

                folder_name = m.group(0)# 0 is the entire match
                folder_path = os.path.join(source_path,folder_name)

                # add "." at the begining of the folder name to make it hidden and prevent it to be simultaneously processed by 
                # another function. Also, create a random name for the destination folder to prevent overwriting
                new_folder_name = f".{folder_name}"
                new_folder_path = os.path.join(source_path,new_folder_name)
                tempo_folder = tempfile.mkdtemp(dir=args.dest)
                os.rename(folder_path, new_folder_path)
                msg = ''
                for dirpath, dirname, filenames  in os.walk(new_folder_path):
                    for f in filenames:
                        if f.endswith(".dcm"):
                            file_path = os.path.join(dirpath,f)
                            ds = pydicom.dcmread(file_path) 

                            # retrieve the classification label
                            label = classify_image_type(ds)

                            ds.DeidentificationMethod = f"{profile}{label}"
                            sop_uid = f.split(".dcm")[0]
                            research_study_id = result.ResearchStudyInfo.study_id
                            rec_id = result.StudyExportRecord.record_id
                            
                            # query to check whether the DICOM file already exists in the "DICOMFilestracking" table
                            q2 = session.query(DICOMFilesTracking).filter(
                                and_(DICOMFilesTracking.study_id == research_study_id, DICOMFilesTracking.study_instance_uid == st_uid, 
                                    DICOMFilesTracking.sop_instance_uid == sop_uid))
                            result_2 = q2.one_or_none() 
                            
                            
                            if result_2 is None:
                                rec = DICOMFilesTracking(study_id=research_study_id, record_id=rec_id, study_instance_uid=st_uid, sop_instance_uid=sop_uid)
                                session.add(rec)
                                session.commit()
                                track_id_value = str(rec.track_id)
                                session.commit()
                                ds.PatientID = result.StudyExportRecord.pseudo_id
                                ds.IssuerOfPatientID = "AUMC^RNG" # This entry allows to retain the patient's name, ID and Clinical Protocol Name
                                ds.ClinicalTrialProtocolName = result.ResearchStudyInfo.study_acronym
                                elem = DataElement("StudyID", 'SH', track_id_value) 
                                if hasattr(ds, "StudyID"):
                                    ds.StudyID = track_id_value 
                                else:
                                    ds.add(elem)

                            # copy the file to the CTP's import directory and remove the remove the original one
                            file_dest = os.path.join(tempo_folder, f) 
                            pydicom.dcmwrite(file_dest, ds)
                            os.remove(file_path)

                # remove the source directory            
                shutil.rmtree(new_folder_path)
                
                received_num = session.query(func.count(DICOMFilesTracking.track_id)).filter(
                    and_(DICOMFilesTracking.study_id == research_study_id, DICOMFilesTracking.study_instance_uid == st_uid)).scalar()

                # if the number of the received "dcm" files is greater than (expected-2), the C-MOVE operation is considered succesfull,
                # otherwise the study record state is set to "PacsReceiveIncomplete" or "Failed" depending on the number of the failures
                if (result.StudyExportRecord.images_in_pacs - received_num) <= 2:
                    new_state = StudyExportState.ExportedToCTP
                    failures = 0
                else:
                    failures = result.StudyExportRecord.number_of_failures + 1
                    if failures > failures_limit:
                        new_state = StudyExportState.Failed
                        msg = 'The requested images were not fully received.'
                        logging.error(f'(Study Instance UID: {st_uid}) {msg}')
                    else:
                        new_state = StudyExportState.PacsReceiveIncomplete
                        msg = 'Currently, the requested images are partly received.'
                        logging.warning(f'(Study Instance UID: {st_uid}) {msg}')
                
                stmt = update(StudyExportRecord).\
                    where(StudyExportRecord.record_id == result.StudyExportRecord.record_id).\
                    values(state = new_state,
                        description = msg,
                        number_of_failures = failures,
                        modified = datetime.now(),
                        images_received = received_num)
                session.execute(stmt)               
                session.commit()

    return None

def ctp_check():

    timelimit = args.timelimit
    failures_limit = args.failures_limit

    time_thresh = datetime.now() - timedelta(hours=timelimit)

    anonymized_uncounted_path = args.anonymized_uncounted
    anonymized_counted_path = args.anonymized_counted
    quarantine_uncounted_path = args.quarantine_uncounted
    quarantine_counted_path = args.quarantine_counted
    unclassified_uncounted_path = args.unclassified_uncounted
    unclassified_counted_path = args.unclassified_counted

    # search the CTP_output directory for "dcm" files
    for dirpath, dirname, filenames in os.walk(anonymized_uncounted_path):
        for f in filenames:
            if f.endswith('.dcm'):
                file_path = os.path.join(dirpath,f)
                ds = pydicom.dcmread(file_path)
                if hasattr(ds, "StudyID"):

                    # extract the (possible) track_id that has been written in the "StudyID" tag.
                    track_id_value = ds.StudyID

                    # query to check if there is a record in the "DICOMFilesTracking" table, which also is in the 
                    # "ExportedToCTP" state, using the track_id_value
                    q = session.query(DICOMFilesTracking).filter(DICOMFilesTracking.track_id == track_id_value)

                    result = q.one_or_none()
                    if result:

                        # update the table with the new state of the file (Anonymized)
                        stmt = update(DICOMFilesTracking).\
                            where(DICOMFilesTracking.track_id == track_id_value).\
                            values(state=CTPExportState.Anonymized)

                        session.execute(stmt)
                        session.commit()

                dir_in_dest_path = f'{anonymized_counted_path}/{dirpath.split("/")[-2]}/{dirpath.split("/")[-1]}'

                if os.path.isdir(dir_in_dest_path) is False:
                    os.makedirs(dir_in_dest_path)

                file_path_dest = os.path.join(dir_in_dest_path, f)
                
                # move the file to the "_9_readys "folder
                shutil.move(file_path, file_path_dest)

                
    # search the quarantine directory for "dcm" files
    for dirpath_2, dirname_2, filenames_2 in os.walk(quarantine_uncounted_path):
        for f_2 in filenames_2:
            if f_2.endswith('.dcm'):
                file_path_2 = os.path.join(dirpath_2,f_2)
                ds_2 = pydicom.dcmread(file_path_2)
                if hasattr(ds_2, "StudyID"):

                    # extract the (possible) track_id that has been written in the "StudyID" tag.
                    track_id_value_2 = ds_2.StudyID

                    # query to check if there is a record in the "DICOMFilesTracking" table, which also is in the 
                    # "ExportedToCTP" state, using the track_id_value_2
                    q2 = session.query(DICOMFilesTracking).filter(DICOMFilesTracking.track_id == track_id_value_2)

                    result_2 = q2.one_or_none()
                    if result_2:

                        # update the table with the new state of the file (InQuarantine)
                        stmt_2 = update(DICOMFilesTracking).\
                            where(DICOMFilesTracking.track_id == track_id_value_2).\
                            values(state=CTPExportState.InQuarantine)

                        session.execute(stmt_2)
                        session.commit()

                dir_in_counted = f'{quarantine_counted_path}/{dirpath_2.split("/")[-2]}/{dirpath_2.split("/")[-1]}'
                
                if os.path.isdir(dir_in_counted) is False:
                    os.makedirs(dir_in_counted)

                file_path_2_dest = os.path.join(dir_in_counted, f_2)
                            
                # move the file to the "counted" folder
                shutil.move(file_path_2, file_path_2_dest)
    
    for dirpath_3, dirname_3, filenames_3 in os.walk(unclassified_uncounted_path):
        for f_3 in filenames_3:
            if f_3.endswith('.dcm'):
                file_path_3 = os.path.join(dirpath_3,f_3)
                ds_3 = pydicom.dcmread(file_path_3)
                if hasattr(ds_3, "StudyID"):

                    # extract the (possible) track_id that has been written in the "StudyID" tag.
                    track_id_value_3 = ds_3.StudyID

                    # query to check if there is a record in the "DICOMFilesTracking" table, which also is in the 
                    q3 = session.query(DICOMFilesTracking).filter(DICOMFilesTracking.track_id == track_id_value_3)

                    result_3 = q3.one_or_none()
                    if result_3:

                        # update the table with the new state of the file 
                        stmt_3 = update(DICOMFilesTracking).\
                            where(DICOMFilesTracking.track_id == track_id_value_3).\
                            values(state=CTPExportState.Unclassified)

                        session.execute(stmt_3)
                        session.commit()

                dir_in_unclassified_counted = f'{unclassified_counted_path}/{dirpath_3.split("/")[-2]}/{dirpath_3.split("/")[-1]}'
                
                if os.path.isdir(dir_in_unclassified_counted) is False:
                    os.makedirs(dir_in_unclassified_counted)

                file_path_3_dest = os.path.join(dir_in_unclassified_counted, f_3)
                            
                # move the file to the "counted" folder
                shutil.move(file_path_3, file_path_3_dest)
 
    # query to select the records which are in the state "ExportedToCTP"    
    q3 = (session.query(ResearchStudyInfo, StudyExportRecord)
        .filter(ResearchStudyInfo.study_id == ImageExportJob.study_id)
        .filter(ImageExportJob.job_id == StudyExportRecord.job_id)
        .filter(or_(StudyExportRecord.state == StudyExportState.ExportedToCTP, StudyExportRecord.state == StudyExportState.Incomplete)))
    
    for i in q3:

        # initialize
        failures = i.StudyExportRecord.number_of_failures
        msg = ''
        modif_time = i.StudyExportRecord.modified 
        new_state = i.StudyExportRecord.state

        # Queries to count the anonymized, quarantine and uclassified files from DICOMFilesTracking.
        anonymized_num = session.query(func.count(DICOMFilesTracking.track_id)).filter(
            and_(DICOMFilesTracking.study_id == i.ResearchStudyInfo.study_id , 
                DICOMFilesTracking.study_instance_uid == i.StudyExportRecord.study_instance_uid, 
                DICOMFilesTracking.state == CTPExportState.Anonymized)).scalar()

        quarantine_num = session.query(func.count(DICOMFilesTracking.track_id)).filter(
            and_(DICOMFilesTracking.study_id == i.ResearchStudyInfo.study_id, 
                DICOMFilesTracking.study_instance_uid == i.StudyExportRecord.study_instance_uid, 
                DICOMFilesTracking.state == CTPExportState.InQuarantine)).scalar()

        unclassified_num = session.query(func.count(DICOMFilesTracking.track_id)).filter(
            and_(DICOMFilesTracking.study_id == i.ResearchStudyInfo.study_id, 
                DICOMFilesTracking.study_instance_uid == i.StudyExportRecord.study_instance_uid, 
                DICOMFilesTracking.state == CTPExportState.Unclassified)).scalar()


        # sum of the files that have been processed by CTP, for this DICOM study
        files_num = anonymized_num + quarantine_num + unclassified_num
        
        # if all the DICOM study's files have been processed, then the study's state is either "completed" (when there are no any 
        # unclassified files) or "Incomplete" (when there are still unclassified files)
        if files_num == i.StudyExportRecord.images_received:
            failures = 0
            modif_time = datetime.now()
            if unclassified_num == 0:
                new_state = StudyExportState.Completed
                   
            else:
                new_state = StudyExportState.Incomplete

        # else, as long as the number of failures is less than the limit, it remains in the "ExportedToCTP" state (and gives a warning) 
        # and once the failures are greater than the limit, it goes to the "Failed" state (and gives a critical error)
        else:
            msg = "The DICOM files in the directory are either 0 or less than expected."
            if i.StudyExportRecord.modified < time_thresh:
                failures = i.StudyExportRecord.number_of_failures + 1
                modif_time = datetime.now()
                if failures < failures_limit:
                    logging.warning(f'(Study Instance UID: {i.StudyExportRecord.study_instance_uid}) {msg}')
                    # no new state
          
                else:
                    new_state = StudyExportState.Failed
                    logging.error(f'(Study Instance UID: {i.StudyExportRecord.study_instance_uid}) {msg}')

        stmt_4 = update(StudyExportRecord).\
            where(StudyExportRecord.record_id == i.StudyExportRecord.record_id).\
                values(state = new_state,
                    description = msg,
                    number_of_failures=failures,
                    modified = modif_time,
                    anonymized_images=anonymized_num,
                    images_in_quarantine=quarantine_num,
                    unclassified_images=unclassified_num)

        session.execute(stmt_4)
        session.commit()

    return None

def update_job_status():

    # query to select the jobs that are in the states "NotStarted" and "Running"
    q = session.query(ImageExportJob).filter(
        or_(ImageExportJob.job_state == ImageExportJobState.NotStarted, 
            ImageExportJob.job_state == ImageExportJobState.Running))

    for i in q:
        
        # queries to count the states that are pending, failed and complete
        pending = session.query(func.count(StudyExportRecord.record_id)).filter(
            and_(StudyExportRecord.job_id == i.job_id, 
                or_(StudyExportRecord.state == StudyExportState.PacsFindDone, 
                    StudyExportRecord.state == StudyExportState.PacsFindFailed,
                    StudyExportRecord.state == StudyExportState.PacsMoveDone, 
                    StudyExportRecord.state == StudyExportState.PacsMoveFailed, 
                    StudyExportRecord.state == StudyExportState.PacsReceiveIncomplete, 
                    StudyExportRecord.state == StudyExportState.ExportedToCTP, 
                    StudyExportRecord.state == StudyExportState.Incomplete))).scalar()
        
        failed = session.query(func.count(StudyExportRecord.record_id)).filter(
            and_(StudyExportRecord.job_id == i.job_id, StudyExportRecord.state == StudyExportState.Failed)).scalar()
        
        complete = session.query(func.count(StudyExportRecord.record_id)).filter(
            and_(StudyExportRecord.job_id == i.job_id, StudyExportRecord.state == StudyExportState.Completed)).scalar()

        if failed + complete == i.total_studies:
            stmt = update(ImageExportJob).\
                where(ImageExportJob.job_id == i.job_id).\
                values(pending_studies=pending,
                    failed_studies=failed,
                    complete_studies=complete,
                    job_state=ImageExportJobState.Completed)

            session.execute(stmt)
            session.commit()
        
        elif pending !=0:
            stmt = update(ImageExportJob).\
                where(ImageExportJob.job_id == i.job_id).\
                values(pending_studies=pending,
                    failed_studies=failed,
                    complete_studies=complete,
                    job_state=ImageExportJobState.Running)

            session.execute(stmt)
            session.commit()

    return None 

def export_reports():

    anonymized_path = args.anonymized_path

    # query to select all the research studies
    q1 = session.query(ResearchStudyInfo.study_acronym, ResearchStudyInfo.study_id)
    
    for i in q1:

        study_path = os.path.join(anonymized_path, i.study_acronym)

        # check if there is a folder with the respective study acronym in the "anonymized" directory
        if os.path.isdir(study_path):
            reports_path = os.path.join(study_path, '_0_reports')
            if os.path.isdir(reports_path) is False:
                os.mkdir(reports_path)

        # query to check whether the study acronym has any jobs in the "Completed" state
        q2 = session.query(ImageExportJob).filter(
            and_(ImageExportJob.study_id == i.study_id, ImageExportJob.job_state == ImageExportJobState.Completed))

        result = q2.all()

        if result:

            # create a csv with all the jobs of the study acronym that are currently in the "Completed" state
            studylevel_filename = f'_0_studylevel_report_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
            studylevel_filename_path = os.path.join(reports_path, studylevel_filename)

            with open(studylevel_filename_path, 'w') as studyfile:
                out1 = csv.writer(studyfile)
                out1.writerow(['job_id', 'total_studies', 'pending_studies', 'failed_studies', 'complete_studies', 'state', 'created'])

                for j, k in enumerate(result):

                    job_id = k.job_id
                    total_studies = k.total_studies
                    pending_studies = k.pending_studies
                    failed_studies = k.failed_studies
                    complete_studies = k.complete_studies
                    job_state = str(k.job_state).split(".")[1]
                    created = k.created 
                    out1.writerow([job_id, total_studies, pending_studies, failed_studies, complete_studies, job_state, created])
                    
                    # create a csv for every job in "Completed" state, which represents the details of all its DICOM studies
                    q3 = session.query(StudyExportRecord).filter(StudyExportRecord.job_id == job_id)

                    joblevel_filename = f'joblevel_jobID_{job_id}_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
                    joblevel_filename_path = os.path.join(reports_path, joblevel_filename)

                    with open(joblevel_filename_path, 'w') as jobfile:
                        out2 = csv.writer(jobfile)
                        out2.writerow(['record_id', 'patient_id', 'accession_number', 'pseudo_id', 'study_instance_uid',
                            'images_received', 'anonymized_images', 'images_in_quarantine', 'state'])

                        for l in q3:
                            record_id = l.record_id
                            patient_id = l.patient_id
                            accession_number = l.accession_number
                            pseudo_id = l.pseudo_id
                            study_instance_uid = l.study_instance_uid
                            images_received = l.images_received
                            anonymized_images = l.anonymized_images
                            images_in_quarantine = l.images_in_quarantine
                            state = str(l.state).split(".")[1]
                            out2.writerow([record_id, patient_id, accession_number, pseudo_id, study_instance_uid, images_received, 
                                anonymized_images, images_in_quarantine, state])

                        jobfile.close()
                    
                    stmt = update(ImageExportJob).\
                        where(ImageExportJob.job_id == job_id).\
                        values(job_state=ImageExportJobState.ExportedToIma, exported=datetime.now())
                    
                    session.execute(stmt)
                    session.commit()

                studyfile.close()

    return None

def remove_old_records():

    # TODO: reset the auto_increment IDs of the tables when possible (e.g. if the table remains totally empty after a deletion)

    clean_studies_after = args.clean_studies_after  
    clean_loggings_after = args.clean_loggings_after  

    loggings_time_thresh = datetime.now() - timedelta(weeks=clean_loggings_after)
    studies_time_thresh = datetime.now() - timedelta(weeks=clean_studies_after)

    # query to delete the records of the files of a DICOM study, once the study has been completed 
    del_files = delete(DICOMFilesTracking).\
        where(DICOMFilesTracking.record_id == StudyExportRecord.record_id).\
        where(or_(StudyExportRecord.state == StudyExportState.Completed, StudyExportRecord.state == StudyExportState.TrackingDetailsRemoved))
    
    session.execute(del_files)

    # update the states in the tables "ImageExportJob" and "StudyExportRecord" after the deletion
    stmt1 = update(StudyExportRecord).\
        where(StudyExportRecord.state == StudyExportState.Completed).\
        where(StudyExportRecord.job_id == ImageExportJob.job_id).\
            values({StudyExportRecord.state : StudyExportState.TrackingDetailsRemoved,
                ImageExportJob.job_state : ImageExportJobState.TrackingDetailsRemoved})

    session.execute(stmt1)

    # query to delete the DICOMStudies of a job, after a given time period (in weeks) after the job has finished
    del_studies = delete(StudyExportRecord).\
        where(StudyExportRecord.job_id == ImageExportJob.job_id).\
        where(or_(and_(
            ImageExportJob.job_state == ImageExportJobState.TrackingDetailsRemoved, ImageExportJob.exported < studies_time_thresh),
            ImageExportJob.job_state == ImageExportJobState.JobDetailsRemoved))
    
    session.execute(del_studies)

    # update the state in the "ImageExportJob" table after the deletion
    stmt2 = update(ImageExportJob).\
        where(ImageExportJob.job_state == ImageExportJobState.TrackingDetailsRemoved).\
        where(ImageExportJob.exported < studies_time_thresh).\
            values(job_state=ImageExportJobState.JobDetailsRemoved)

    session.execute(stmt2)

    # delete all the records of "LoggingMessages" table after a given time period (in weeks)
    del_loggings = delete(LoggingMessages).where(LoggingMessages.created < loggings_time_thresh)
    session.execute(del_loggings)    

    session.commit()

    return None


def extract_image_info():

    source_path = args.source
    archive_path = args.archive
    dirs = os.listdir(source_path)

    # check if the names of the sub-directories ("safe", "unsafe", "burned_in") has been modified/removed
    if all(elem in dirs for elem in label_dirs):
        dirs = label_dirs
    else:
        msg = 'The sub-directories within the source directory do not have the proper names or they are missing'
        '(see the "classification_directories" list in dicts.py). They probably have been modified/removed.'
        logging.error(msg)  
        raise KeyError(msg)
    
    # check the three directories for "dcm" files
    for label in dirs:
        for dirpath, dirname, filenames  in os.walk(os.path.join(source_path, label)):
            for f in filenames:
                if f.endswith('.dcm'):
                    file_path = os.path.join(dirpath,f)
                    ds = pydicom.dcmread(file_path)

                    # extract the values of a set of DICOM tags from the file's header
                    ds_dict = make_dictionary(ds)
                    
                    # query to check in the "ImageTypeClassification" table if there is a record with the same values 
                    # in the respective DICOM tags
                    q = (session.query(ImageTypeClassification, ClassifiedImagesCatalog)
                        .filter(ImageTypeClassification.type_id == ClassifiedImagesCatalog.class_id)
                        .filter(
                            and_(
                                ImageTypeClassification.sop_class_uid == ds_dict["sop_class_uid"],
                                ImageTypeClassification.image_type == ds_dict["image_type"],
                                ImageTypeClassification.manufacturer == ds_dict["manufacturer"],
                                ImageTypeClassification.software_versions == ds_dict["software_versions"],
                                ImageTypeClassification.secondary_capture_device_manufacturer == ds_dict["secondary_capture_device_manufacturer"],
                                ImageTypeClassification.secondary_capture_device_manufacturer_model_name == ds_dict["secondary_capture_device_manufacturer_model_name"],
                                ImageTypeClassification.secondary_capture_device_software_versions == ds_dict["secondary_capture_device_software_versions"])))
                    
                    result = q.one_or_none()  

                    # if there is no match, add a new record to the table with the respective DICOM tags values and 
                    # the label that corresponds to the current directory                   
                    if result is None:
                        im = ImageTypeClassification(
                            sop_class_uid = ds_dict["sop_class_uid"], 
                            image_type = ds_dict["image_type"], 
                            manufacturer = ds_dict["manufacturer"], 
                            software_versions = ds_dict["software_versions"],
                            secondary_capture_device_manufacturer = ds_dict["secondary_capture_device_manufacturer"], 
                            secondary_capture_device_manufacturer_model_name = ds_dict["secondary_capture_device_manufacturer_model_name"],
                            secondary_capture_device_software_versions = ds_dict["secondary_capture_device_software_versions"], 
                            image_type_class = getattr(ImageTypeClassLabel, im_type_dict[label]))                 

                        # add the label's value in the header, in the private tag (1717, 9999)
                        im.ClassifiedImagesCatalog = [ClassifiedImagesCatalog(file_name=f)]
                        session.add(im)
                        elem = DataElement(0x17179999, 'SH', im_type_dict[label])
                        ds.add(elem)
                        new_file_path = os.path.join(archive_path, label, f)
                        pydicom.dcmwrite(new_file_path, ds)
                        os.remove(file_path)
                    
                    else: # (if the same record exists)

                        previous_file_src = os.path.join(archive_path, label, result.ClassifiedImagesCatalog.file_name)
                        previous_file_dest = os.path.join(args.old_files, label, result.ClassifiedImagesCatalog.file_name)
                        
                        # NOTE: the three labels follow the hierarchy "unsafe-> burned-in-> safe" 
                        # (unsafe overules burned-in, which in turn overules safe) 

                        if result.ImageTypeClassification.image_type_class == ImageTypeClassLabel.BurnedIn:

                            # case: the existing record is "burned-in" and the new file is "unsafe".
                            if label == "unsafe":

                                # update the record with the name of the new file's name and the "unsafe" label
                                stmt = update(ImageTypeClassification).\
                                    where(ImageTypeClassification.type_id == result.ImageTypeClassification.type_id).\
                                    where(result.ImageTypeClassification.type_id == result.ClassifiedImagesCatalog.class_id).\
                                    where(ClassifiedImagesCatalog.class_id == result.ClassifiedImagesCatalog.class_id).\
                                    values({
                                        ImageTypeClassification.image_type_class : getattr(ImageTypeClassLabel, im_type_dict[label]),
                                        ClassifiedImagesCatalog.file_name : f})
                                session.execute(stmt)

                                # add the label to the header
                                elem = DataElement(0x17179999, 'SH', im_type_dict[label])
                                ds.add(elem)

                                # archive the new file
                                new_file_path = os.path.join(archive_path, label, f)
                                pydicom.dcmwrite(new_file_path, ds)

                                # archive the old file
                                shutil.move(previous_file_src, previous_file_dest)

                        elif result.ImageTypeClassification.image_type_class == ImageTypeClassLabel.Safe:

                            # case: the existing record is "safe", so if the file is not "safe", then update
                            if label != "safe":
                                stmt = update(ImageTypeClassification).\
                                    where(ImageTypeClassification.type_id == result.ImageTypeClassification.type_id).\
                                    where(result.ImageTypeClassification.type_id == result.ClassifiedImagesCatalog.class_id).\
                                    where(ClassifiedImagesCatalog.class_id == result.ClassifiedImagesCatalog.class_id).\
                                    values({
                                        ImageTypeClassification.image_type_class : getattr(ImageTypeClassLabel, im_type_dict[label]),
                                        ClassifiedImagesCatalog.file_name : f})
                                session.execute(stmt) 

                                # add the label to the header
                                elem = DataElement(0x17179999, 'SH', im_type_dict[label])
                                ds.add(elem)

                                # archive the new file
                                new_file_path = os.path.join(archive_path, label, f)    
                                pydicom.dcmwrite(new_file_path, ds)

                                # archive the old file
                                shutil.move(previous_file_src, previous_file_dest)   
                        os.remove(file_path)
                    session.commit()

    return None

def classify_image_type(dicom_header): # NOTE: This is the only function in this script which is called internally (by prepare_session function)
    
    # extract the values of a set of DICOM tags from the file's header
    ds_dict = make_dictionary(dicom_header)

    # query to check in the "ImageTypeClassification" table if there is a record with the same values 
    # in the respective DICOM tags
    q = session.query(ImageTypeClassification).filter(
        and_(
            ImageTypeClassification.sop_class_uid == ds_dict["sop_class_uid"],
            ImageTypeClassification.image_type == ds_dict["image_type"],
            ImageTypeClassification.manufacturer == ds_dict["manufacturer"],
            ImageTypeClassification.software_versions == ds_dict["software_versions"],
            ImageTypeClassification.secondary_capture_device_manufacturer == ds_dict["secondary_capture_device_manufacturer"],
            ImageTypeClassification.secondary_capture_device_manufacturer_model_name == ds_dict["secondary_capture_device_manufacturer_model_name"],
            ImageTypeClassification.secondary_capture_device_software_versions == ds_dict["secondary_capture_device_software_versions"]))

    result = q.one_or_none()

    # if there is no match, leave the ladel empty (the file then will be forwarded to the "unclassified" folder)
    if result is None:
        label = ""
    
    # if there is a match, extract the classification label from the header
    else:
        label = (str(result.image_type_class)).split(".")[1]

    return label


if __name__ == "__main__":

    session = Session()

    # Load the configuration files
    config = configparser.ConfigParser()
    config.read("/opt/amc/mies/config_files/args_config_dcmhub.ini")
    logging.config.fileConfig("/opt/amc/mies/config_files/logs_config_dcmhub.conf")
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine')

    # Create parsers and subparsers 
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='proccesses')
    
    studymaker = subparsers.add_parser('study',
        help='Looks into a pre-defined directory for "xls" files with a pre-specified structure format. By extracting '
        'the relevant information from these files, it creates one or more new study_records (if not already exist) and '
        'submits them to the DataBase.')

    studymaker_subparsers = studymaker.add_subparsers(title='mode')

    studymaker_auto = studymaker_subparsers.add_parser('auto')
    studymaker_auto.set_defaults(func=create_new_research_study_auto)
    studymaker_auto.add_argument('--source', default=config['STUDY']['SOURCE'], 
        help='The source directory where the function looks for "xls" files. By extracting the relevant information from '
        'these files, it creates one or more job records (related to some studies) and submits them to the DataBase.')
    studymaker_auto.add_argument('--archive', default=config['STUDY']['ARCHIVE'],
        help='The directory wherein the submitted "xls" files are archived.')        
    
    studymaker_manual = studymaker_subparsers.add_parser('manual')
    studymaker_manual.set_defaults(func=create_new_research_study_manual)
    studymaker_manual.add_argument('study_acronym', 
        help='The name of the research study')
    studymaker_manual.add_argument('email_address', 
        help='The email of the researcher of the study')
    studymaker_manual.add_argument('--no_cln_graphics', 
        action='store_false',
        help='Disables the Clean Graphics option. When this option is specified in addition to an Application Level '
        'Confidentiality Profile, any information encoded in graphics, text annotations or overlays corresponding to '
        'the Attribute information specified to be removed by the Profile and any other '
        'options specified shall also be removed.') 
    studymaker_manual.add_argument('--no_cln_structured_content', 
        action='store_false',
        help='Disables the Clean Structured Content option. When this Option is specified in addition to an Application '
        'Level Confidentiality Profile, any information encoded in SR Content Items or Acquisition Context or Specimen '
        'Preparation Sequence Items corresponding to the Attribute information specified to be removed by the Profile and '
        'any other options specified shall also be removed.')
    studymaker_manual.add_argument('--no_cln_descriptors', 
        action='store_false',
        help='Disables the Clean Descriptors option. When this Option is specified in addition to an Application Level '
        'Confidentiality Profile, any information that is embedded in text or string Attributes corresponding to the Attribute '
        'information specified to be removed by the Profile and any other Options specified shall also be removed.')
    studymaker_manual.add_argument('--rtn_longitudinal_full_dates', 
        action='store_true',
        help='Enables the Retain Longitudinal Temporal Information With Full Dates option. When this option is specified in '
        'addition to an Application Level Confidentiality Profile, any dates and times present in the Attributes shall be retained. '
        'The Attribute Longitudinal Temporal Information Modified (0028,0303) shall be added to the Dataset with a value of "UNMODIFIED".')
    studymaker_manual.add_argument('--rtn_longitudinal_modified_dates', 
        action='store_true', 
        help='Enables the Retain Longitudinal Temporal Information With Modified Dates option. When this option is specified '
        'in addition to an Application Level Confidentiality Profile, any dates and times present in the Attributes shall be modified. '
        'The modification of the dates and times shall be performed in a manner that: \n aggregates or transforms dates so as to reduce '
        'the possibility of matching for re-identification \n preserves the gross longitudinal temporal relationships between images '
        'obtained on different dates \n to the extent necessary for the application \n preserves the fine temporal relationships between '
        'images and real-world events to the extent necessary for analysis of the images for the application. \n The Attribute Longitudinal '
        'Temporal Information Modified (0028,0303) shall be added to the Dataset with a value of "MODIFIED".')
    studymaker_manual.add_argument('--rtn_patient_characteristics', 
        action='store_true',
        help='Enables the Retain Patient Characteristics option. When this Option is specified in addition to an Application Level '
        'Confidentiality Profile, information about age, sex, height and weight and other characteristics present in the Attributes '
        'shall be retained.')
    studymaker_manual.add_argument('--rtn_device_id', 
        action='store_true',
        help='Enables the Retain Device Identity option. When this Option is specified in addition to an Application Level '
        'Confidentiality Profile, information about the identity of the device in the Attributes shall be retained')
    studymaker_manual.add_argument('--rtn_uids', 
        action='store_true',
        help='Enables the Retain UIDs option. When this Option is specified in addition to an Application Level Confidentiality Profile, '
        'UIDs shall be retained.')
    studymaker_manual.add_argument('--rtn_safe_private', 
        action='store_true', 
        help='Enables the Retain Safe Private option. When this Option is specified in addition to an Application Level Confidentiality '
        'Profile, Private Attributes that are known by the de-identifier to be safe from identity leakage shall be retained, together with '
        'the Private Creator IDs that are required to fully define the retained Private Attributes; all other Private Attributes shall be '
        'removed or processed in the element-specific manner recommended by Deidentification Action (0008,0307), if present within Private '
        'Data Element Characteristics Sequence (0008,0300).')
    studymaker_manual.add_argument('--rtn_institution_id', 
        action='store_true', 
        help='Enables the  Retain Institution Identity option. When this Option is specified in addition to an Application Level '
        'Confidentiality Profile, information about the identity of the institution in the Attributes shall be retained')

    jobmaker = subparsers.add_parser('job',
        help='Looks into a pre-defined directory for "xls" files with a pre-specified structure format. If it finds new DICOM studies that '
        'are related to an excisting study acronym, then it creates a new job and submit it to the DataBase')
    jobmaker.set_defaults(func=create_new_job)
    jobmaker.add_argument('--source', default=config['JOB']['SOURCE'], 
        help='The source directory where the function looks for "xls" files.')
    jobmaker.add_argument('--archive', default=config['JOB']['ARCHIVE'],
        help='The directory wherein the submitted "xls" files are archived.')

    finder = subparsers.add_parser('find', 
        help='Applies the C-FIND service operation by which relevant patient information is queried by the user AE and provided by the PACS.')
    finder.set_defaults(func=find_session)
    finder.add_argument('--hostname', default=config['FIND']['HOSTNAME'], 
        help='The PACS server name or IP address')
    finder.add_argument('--port', default=config['FIND']['PORT'], type=int, 
        help='The PACS server port that is used for the connection')
    finder.add_argument('--host_ae_title', default=config['FIND']['HOST_AE_TITLE'], 
        help='The PACS Application Entity title')
    finder.add_argument('--user_ae_title', default=config['FIND']['USER_AE_TITLE'], 
        help='The user Application Entity title')
    finder.add_argument('--query_retrieve_level', default=config['FIND']['QUERY_RETRIEVE_LEVEL'], 
        help='DICOM level where the C-FIND operation is applied')
    finder.add_argument('--timelimit', default=config['FIND']['RETRY_INTERVAL'], type=float,
        help='This argument indicates a time interval, which is used from the function in order to select the records in the DB which have '
        'been modified before this.')
    finder.add_argument('--failures_limit', default=config['FIND']['FAILURES_LIMIT'], type=int,
        help='This argument indicates the maximum number of the failures a record in the DB is allowed to have, before it is set to the '
        '"Failed" state.')
    
    mover = subparsers.add_parser('move', 
        help='Applies the C-MOVE service operation which allows the user AE to instruct PACS to transfer stored SOP Instances to another AE.')
    mover.set_defaults(func=move_session)
    mover.add_argument('--hostname', default=config['MOVE']['HOSTNAME'], 
        help='The PACS server name or IP address')
    mover.add_argument('--port', type=int, default=config['MOVE']['PORT'], 
        help='The PACS server port that is used for the connection')
    mover.add_argument('--host_ae_title', default=config['MOVE']['HOST_AE_TITLE'], 
        help='The PACS Application Entity title')
    mover.add_argument('--user_ae_title', default=config['MOVE']['USER_AE_TITLE'], 
        help='The user Application Entity title')
    mover.add_argument('--receiver_ae_title', default=config['MOVE']['RECEIVER_AE_TITLE'], 
        help='The receiver Application Entity title')
    mover.add_argument('--query_retrieve_level', default=config['MOVE']['QUERY_RETRIEVE_LEVEL'], 
        help='DICOM level where the C-MOVE operation is applied')
    mover.add_argument('--active_jobs_limit', default=config['MOVE']['ACTICE_JOBS_LIMIT'], type=int,
        help='The argument that limits the actve jobs')
    mover.add_argument('--timelimit', default=config['MOVE']['RETRY_INTERVAL'], type=float,
        help='This argument indicates a time interval, which is used from the function in order to select the records in the DB which have been '
        'modified before this.')
    mover.add_argument('--failures_limit', default=config['MOVE']['FAILURES_LIMIT'], type=int,
        help='This argument indicates the maximum number of the failures a record in the DB is allowed to have, before it is set to the '
        '"Failed" state.')
    mover.add_argument('--output_path', default=config['MOVE']['OUTPUT_PATH'], 
        help='The directory where PACS outputs the requested studies')
    
    preparer = subparsers.add_parser('prepare',
        help='Prepares the DICOM files for the CTP pipeline, by modifying some specific tags. At the same time 1) it enumerates them and '
        'updates the "StudyExportRecord" table and 2) it adds a record to the "DICOMFilesTracking" table with their SOP Instance UID, as well as '
        'the respective Study Instance UID.')
    preparer.set_defaults(func=prepare_session)
    preparer.add_argument('--source', default=config['PREPARE']['SOURCE'], 
        help='The source directory where the DICOM files are retrieved. Usually it is the directory where the files are stored after the C-MOVE '
        'service operation.')
    preparer.add_argument('--dest', default=config['PREPARE']['DEST'], 
        help='The destination directory where the files are stored once they have been prepared.')
    preparer.add_argument('--timelimit', default=config['PREPARE']['RETRY_INTERVAL'], type=float,
        help='This argument indicates a time interval, which is used from the function in order to select the records in the DB which have been '
        'modified before this.')
    preparer.add_argument('--failures_limit', default=config['PREPARE']['FAILURES_LIMIT'], type=int,
        help='This argument indicates the maximum number of the failures a record in the DB is allowed to have, before it is set to the '
        '"Failed" state.')

    ctpchecker = subparsers.add_parser('ctpcheck',
        help="It searches the CTP's output directories (anonymized and quarantine) for dcm files. For every dcm file checks if exists in the DB. "
        "If so, it updates the StudyExportRecord table by increasing the number of the images that were anonynized or in quarantine by CTP and "
        'forwards the file to the "counted" directory. Additionally, it updates the DICOMFilesTracking table with the state of every file. '
        'The function also forwards all the dcm files that not exist in the DB in the "counted" directory.')
    ctpchecker.set_defaults(func=ctp_check)
    ctpchecker.add_argument('--anonymized_uncounted', default=config['CTPCHECK']['ANONYMIZED_UNCOUNTED'],
        help='The directory where CTP outputs the anonymized files.')
    ctpchecker.add_argument('--anonymized_counted', default=config['CTPCHECK']['ANONYMIZED_COUNTED'], 
        help='The directory where the anonymized files are being forwarded.')
    ctpchecker.add_argument('--quarantine_uncounted', default=config['CTPCHECK']['QUARANTINE_UNCOUNTED'], 
        help='The directory where CTP outputs all the unsafe files (quarantine).')
    ctpchecker.add_argument('--quarantine_counted', default=config['CTPCHECK']['QUARANTINE_COUNTED'], 
        help='The directory where the unsafe files are forwarded to once they have been counted.')
    ctpchecker.add_argument('--unclassified_uncounted', default=config['CTPCHECK']['UNCLASSIFIED_UNCOUNTED'], 
        help='The directory where CTP outputs all the unclassified files.')
    ctpchecker.add_argument('--unclassified_counted', default=config['CTPCHECK']['UNCLASSIFIED_COUNTED'], 
        help='The directory where the unclassified files are forwarded to once they have been counted.')
    ctpchecker.add_argument('--timelimit', default=config['CTPCHECK']['RETRY_INTERVAL'], type=float,
        help='This argument indicates a time interval, which is used from the function in order to select the records in the DB which have been '
        'modified before this.')
    ctpchecker.add_argument('--failures_limit', default=config['CTPCHECK']['FAILURES_LIMIT'], type=int,
        help='This argument indicates the maximum number of the failures a record in the DB is allowed to have, before it is set to the '
        '"Failed" state.')

    jobupdater = subparsers.add_parser('update_job',
        help='Updates the status of "ImageExportJob" table by examinating the status of the DICOM studies in "StudyExportRecord" table. '
            'This function takes no arguments.') 
    jobupdater.set_defaults(func=update_job_status)    

    reportexporter = subparsers.add_parser('export_reports',
        help='Queries the DB to select the completed jobs, then looks into the folder with the anonymized studies to check if a folder '
        'for each respective study_acronym exists in the directory and, if so, extracts "csv" reports for the research study, as well as the jobs.')
    reportexporter.set_defaults(func=export_reports)
    reportexporter.add_argument('--anonymized_path', default=config['EXPORTREPORTS']['ANONYMIZED_PATH'],
        help='Path wherein the anonymized DICOM studies of each Research study are stored.')

    databasecleaner = subparsers.add_parser('clean', 
        help='Removes the old records from the tables "StudyExportRecord" and "LoggingMessages" according to pre-defined time limits and all the '
        'records from the table "DICOMFilesTracking" related to the studies of a job, once the job has finished.')
    databasecleaner.set_defaults(func=remove_old_records)
    databasecleaner.add_argument('--clean_studies_after', default=config['REMOVE_OLD_RECORDS']['CLEAN_STUDIES_AFTER'], type=int,
        help='Time period (in weeks) after which the records of all the DICOM studies of a specic job are removed.')
    databasecleaner.add_argument('--clean_loggings_after', default=config['REMOVE_OLD_RECORDS']['CLEAN_LOGGINGS_AFTER'], type=int,
        help='Time period (in weeks) after which all the records from the table "LoggingMessages" are removed.')
    
    extractor = subparsers.add_parser('extract',
        help='Looks into three pre-defined directories for new DICOM files and then extracts relevant information from the DICOM header. '
        'Then, makes a query to the database to check if this information matches to any of the existing records. If not, it creates a '
        'new record and additionally, adds a classification label to the new record (according to which of the three directories the file '
        'comes from) so it can be used in the regular workflow to automatically classify similar images that have matching DICOM attributes. '
        'The files that correspond to new records are being archived into a specific folder.')
    extractor.set_defaults(func=extract_image_info)
    extractor.add_argument('--source', default=config['EXTRACTIMAGEINFO']['SOURCE'],
        help='The source directory where the classified files are.')
    extractor.add_argument('--archive', default=config['EXTRACTIMAGEINFO']['ARCHIVE'],
        help='The folder where the files that correspond to the new records are stored.')
    extractor.add_argument('--old_files', default=config['EXTRACTIMAGEINFO']['OLD_FILES'],
        help='The folder where the old files are stored in.')

    args =  parser.parse_args()   
    args.func() 

