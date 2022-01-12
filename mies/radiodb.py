__author__ = "Gerasimos matidis"

# This script contains the definition and creation of the MIES database

import enum
from datetime import datetime
from sqlalchemy import *
from sqlalchemy import Index
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker, relation
from logging import StreamHandler, Handler
import configparser

Base = declarative_base()


# define the states which are used by each table
class ImageExportJobState(enum.Enum):
    NotStarted = 1
    Running = 2
    Completed = 3
    ExportedToIma = 4
    TrackingDetailsRemoved = 5
    JobDetailsRemoved = 6

class StudyExportState(enum.Enum):
    NotStarted = 1
    PacsFindDone = 2
    PacsFindFailed = 3
    PacsMoveDone = 4
    PacsMoveFailed = 5
    PacsReceiveIncomplete = 6
    ExportedToCTP = 7
    Incomplete = 8
    Completed = 9
    Failed = 10
    TrackingDetailsRemoved = 11

class CTPExportState(enum.Enum):
    Anonymized = 1
    InQuarantine = 2 
    Unclassified = 3

class LogLevel(enum.Enum):
    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4

class ImageTypeClassLabel(enum.Enum):
    Safe = 1
    Unsafe = 2
    BurnedIn = 3

# define the tables
class ResearchStudyInfo(Base):
    __tablename__ = 'ResearchStudyInfo'
    __table_args__ = {'mysql_engine':'InnoDB'}

    study_id = Column(Integer, primary_key=True)
    study_acronym = Column(String(32), nullable=False)
    researchers = Column(String(32), nullable=False) 
    cln_graphics = Column(Boolean, nullable=False, default=True)
    cln_structured_content = Column(Boolean)
    cln_descriptors = Column(Boolean)
    rtn_longitudinal_full_dates = Column(Boolean)
    rtn_longitudinal_modified_dates = Column(Boolean)
    rtn_patient_characteristics = Column(Boolean)
    rtn_device_id = Column(Boolean)
    rtn_uids = Column(Boolean)
    rtn_safe_private = Column(Boolean)
    rtn_institution_id = Column(Boolean)  
    
class ImageExportJob(Base):
    __tablename__ = 'ImageExportJob'
    __table_args__ = {'mysql_engine':'InnoDB'}

    job_id = Column(Integer, primary_key=True)
    study_id = Column(Integer, ForeignKey('ResearchStudyInfo.study_id'))
    total_studies = Column(Integer, default=0)
    pending_studies = Column(Integer, default=0)
    failed_studies = Column(Integer, default=0)
    complete_studies = Column(Integer, default=0)
    job_state = Column(Enum(ImageExportJobState), nullable=False, default=ImageExportJobState.NotStarted)
    created = Column(DateTime, nullable=False, default=datetime.now())
    exported = Column(DateTime, nullable=True, default=None)

class StudyExportRecord(Base):
    __tablename__ = 'StudyExportRecord'
    __table_args__ = {'mysql_engine':'InnoDB'}

    record_id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('ImageExportJob.job_id'))
    patient_id = Column(String(64), nullable=False)
    pseudo_id = Column(String(64), nullable=False)
    patient_birth_date = Column(String(8), nullable=True, default=None)
    accession_number = Column(String(16), nullable=False)
    study_description = Column(String(64), nullable=True, default=None)
    study_instance_uid = Column(String(64), nullable=True, default=None)
    study_date = Column(String(8), nullable=True, default=None)
    study_time = Column(String(16), nullable=True, default=None)
    state = Column(Enum(StudyExportState), nullable=False, default=StudyExportState.NotStarted)
    description = Column(String(256), nullable=True, default=None)
    number_of_failures = Column(Integer, nullable=True, default=0)
    modified = Column(DateTime, nullable=False, default=datetime.now())
    images_in_pacs = Column(Integer, default=0)
    images_received = Column(Integer, default=0)
    anonymized_images = Column(Integer, default=0)
    images_in_quarantine = Column(Integer, default=0)
    unclassified_images = Column(Integer, default=0)
    modalities_in_study = Column(String(32), nullable=True, default=None)
    station_name = Column(String(32), nullable=True, default=None)
    exp_job = relation('ImageExportJob', backref='StudyExportRecord', lazy=False)

Index("st_uid_idx", StudyExportRecord.study_instance_uid)

class DICOMFilesTracking(Base):
    __tablename__ = 'DICOMFilesTracking'
    __table_args__ = {'mysql_engine':'InnoDB'}

    track_id = Column(Integer, primary_key=True)
    study_id = Column(Integer, ForeignKey('ResearchStudyInfo.study_id'))
    record_id = Column(Integer, ForeignKey('StudyExportRecord.record_id'))
    sop_instance_uid = Column(String(128), nullable=False)
    study_instance_uid = Column(String(64), nullable=True, default=None)
    state = Column(Enum(CTPExportState), nullable=True, default=None)

Index("st_id_plus_st_uid_idx", DICOMFilesTracking.study_id, DICOMFilesTracking.study_instance_uid)

class LoggingMessages(Base):
    __tablename__ = 'LoggingMessages'
    __table_args__ = {'mysql_engine':'InnoDB'}

    log_id = Column(Integer, primary_key=True)
    created = Column(DateTime, nullable=False, default=datetime.now())
    log_level = Column(Enum(LogLevel), nullable=True, default=None)
    function = Column(String(32), nullable=True, default=None)
    message = Column(String(256), nullable=False)

class ImageTypeClassification(Base):
    __tablename__ = 'ImageTypeClassification'
    __table_args__ = {'mysql_engine':'InnoDB'}

    type_id = Column(Integer, primary_key=True)
    sop_class_uid = Column(String(64), nullable=True, default=None)
    image_type = Column(String(256), nullable=True, default=None)
    manufacturer = Column(String(64), nullable=True, default=None)
    software_versions = Column(String(64), nullable=True, default=None)
    secondary_capture_device_manufacturer = Column(String(64), nullable=True, default=None)
    secondary_capture_device_manufacturer_model_name = Column(String(64), nullable=True, default=None)
    secondary_capture_device_software_versions = Column(String(64), nullable=True, default=None)
    image_type_class = Column(Enum(ImageTypeClassLabel), nullable=True, default=None) 

Index("sop_class_uid_idx", ImageTypeClassification.sop_class_uid)

class ClassifiedImagesCatalog(Base):
    __tablename__ = 'ClassifiedImagesCatalog'
    __table_args__ = {'mysql_engine':'InnoDB'}

    class_id = Column(Integer, primary_key=True)
    type_id = Column(Integer, ForeignKey('ImageTypeClassification.type_id'))
    file_name = Column(String(128), nullable=True, default=None)
    exp_path = relation('ImageTypeClassification', backref='ClassifiedImagesCatalog', lazy=False)

# class that writes the logging records to the "LoggingMessages" table
class DataBaseHandler(StreamHandler):
    "A handler class which writes formatted logging records to a DataBase."
    def __init__(self):
        Handler.__init__(self)
        self.stream = None
        self.session = Session()

    def emit(self, record):
        info = self.format(record)
        level, function, message = info.split("|")
        level = getattr(LogLevel, level)
        log_record = LoggingMessages(log_level=level, function=function, message=message)
        try:
            self.session.add(log_record)
            self.session.commit()
        except Exception:
            print("Logging to DataBase was not possible!")
        
        return None


config = configparser.ConfigParser()
config.read('/opt/amc/mies/config_files/args_config_dcmhub.ini')
database = config['DATABASE']['PASSWORD']
encoding = config['DATABASE']['ENCODING']
echo= eval(config['DATABASE']['ECHO'])

# Create a DataBase (if not exists) and start a new session
engine = create_engine(database, encoding=encoding, echo=echo)
meta = Base.metadata
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
