__author__ = "Gerasimos Matidis"

# This scripts contains the functions that interact with PACS (C-FIND, C_MOVE)

import sys
from pynetdicom import AE
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind, StudyRootQueryRetrieveInformationModelMove
from pynetdicom.status import QR_MOVE_SERVICE_CLASS_STATUS as move_status_dict
from pynetdicom.status import QR_FIND_SERVICE_CLASS_STATUS as find_status_dict
from pydicom.dataset import Dataset

def pacs_find(hostname, port, host_ae_title, user_ae_title, query_retrieve_level, accession_number, patient_id):
    """Makes a query to PACS to find DICOM studies

    Parameters
    ----------
    hostname : str
        The peer Application Entity's TCP/IP address.
    port : int
        The peer AE's listen port number.
    host_ae_title : bytes
        The peer's AE title, will be used as the *Called AE Title* parameter value.
    user_ae_title : bytes
        The user's AE title, will be used as the *Calling AE Title* parameter value.
    query_retrieve_level : str
        The DICOM level of the retrieval.
    accession_number : str
        DICOM tag wich is used as argument to the query.
    patient_id : str
        DICOM tag wich is used as argument to the query.

    Returns
    -------
    dictionary
        The requested DICOM Data Elements
    int
        The number of the matches of the query
    str
        The message with the response
    """

    ae = AE(ae_title=user_ae_title)
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)
    ds = Dataset()
    
    # query's arguments
    ds.AccessionNumber = accession_number
    ds.PatientID = patient_id

    # requested DICOM data elements
    ds.PatientBirthDate = ''
    ds.StudyDescription = ''
    ds.StudyInstanceUID = ''
    ds.StudyDate = ''
    ds.StudyTime = ''
    ds.ModalitiesInStudy = ''
    ds.StationName = ''
    ds.NumberOfStudyRelatedInstances = ''
    ds.QueryRetrieveLevel = query_retrieve_level
    assoc = ae.associate(hostname, port, ae_title=host_ae_title)
    
    a = None
    matches = 0
    msg = None
    if assoc.is_established:
        responses = assoc.send_c_find(ds,StudyRootQueryRetrieveInformationModelFind)
        for (status, identifier) in responses:
            if matches == 2:
                a = None
                ae.shutdown()
                msg = 'Multiple studies found for this query.'
                break
            if status:
                msg = find_status_dict[status.Status][1]
                if status.Status in (0xFF00, 0xFF01):    
                    a = identifier
                    matches += 1
                if status.Status == 0x0000 and a is None:
                    msg = 'No study found for this query.'                
            else:
                msg = 'Connection timed out, was aborted or received invalid response.'
        assoc.release()
    else:
        msg = 'Association rejected, aborted or never connected.' 
        matches = None
    return a, matches, msg

def pacs_move(hostname, port, host_ae_title, user_ae_title, receiver_ae_title, query_retrieve_level, study_instance_uid): 
    """Makes a query to PACS to find DICOM studies

    Parameters
    ----------
    hostname : str
        The peer Application Entity's TCP/IP address.
    port : int
        The peer AE's listen port number.
    host_ae_title : bytes
        The peer's AE title, will be used as the *Called AE Title* parameter value.
    user_ae_title : bytes
        The user's AE title, will be used as the *Calling AE Title* parameter value.
    receiver_ae_title : bytes
        The receiver's AE title.
    query_retrieve_level : str
        The DICOM level of the retrieval.
    study_instance_uid : str
        DICOM tag wich is used as argument to the query.

    Returns
    -------
    str
        The message with the response
    """
    ae = AE(ae_title=user_ae_title)
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)
    ds = Dataset()
    ds.QueryRetrieveLevel = query_retrieve_level

    # query's argument
    ds.StudyInstanceUID = study_instance_uid
    assoc = ae.associate(hostname, port, ae_title=host_ae_title)
    msg = None
    if assoc.is_established:
        responses = assoc.send_c_move(ds, receiver_ae_title, StudyRootQueryRetrieveInformationModelMove)    
        for (status, identifier) in responses:
            if status:
                msg = move_status_dict[status.Status][1]
            else:
                msg = 'Connection timed out, was aborted or received invalid response.' 
        assoc.release()
    else:
        msg = 'Association rejected, aborted or never connected.'
    return msg
