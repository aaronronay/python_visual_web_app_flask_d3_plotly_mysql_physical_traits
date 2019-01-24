#dependencies
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, desc

#read files
meta_df = pd.read_csv("./DataSets/Belly_Button_Biodiversity_Metadata.csv")
otu_df = pd.read_csv("./DataSets/belly_button_biodiversity_otu_id.csv")
samples_df = pd.read_csv("./DataSets/belly_button_biodiversity_samples.csv")
columns_df = pd.read_csv("./DataSets/metadata_columns.csv")

#SQL
engine = create_engine("sqlite:///DataSets/belly_button_biodiversity.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)
Otu = Base.classes.otu
Samples = Base.classes.samples
Samples_metadata = Base.classes.samples_metadata
session = Session(engine)
samples = Samples.__table__.columns
samples_list = [sample.key for sample in samples]
samples_list.remove("otu_id")

#query everything, get descriptions
otu_descriptions = session.query(Otu.lowest_taxonomic_unit_found).all()
otu_descriptions_list = [x for (x), in otu_descriptions]

#function that extracts metadata for sample
def sample_query(sample):
    sample_name = sample.replace("BB_", "")
    result = session.query(Samples_metadata.AGE, Samples_metadata.BBTYPE, Samples_metadata.ETHNICITY, Samples_metadata.GENDER, Samples_metadata.LOCATION, Samples_metadata.SAMPLEID).filter_by(SAMPLEID = sample_name).all()
    record = result[0]
    record_dict = {
        "AGE": record[0],
        "BBTYPE": record[1],
        "ETHNICITY": record[2],
        "GENDER": record[3],
        "LOCATION": record[4],
        "SAMPLEID": record[5] }
    return (record_dict)

# get wash frequency
wash_freq = result[0][0]

#function that extracts otu data for sample
def otu_data(sample):
    sample_query = "Samples." + sample
    result = session.query(Samples.otu_id, sample_query).order_by(desc(sample_query)).all()
    otu_ids = [result[x][0] for x in range(len(result))]   
    sample_values = [result[x][1] for x in range(len(result))]
    dict_list = [{"otu_ids": otu_ids}, {"sample_values": sample_values}]
    return dict_list

#create OTU dictionary
otu_dict = {}
otu_descriptions = session.query(Otu.otu_id, Otu.lowest_taxonomic_unit_found).all()
for row in otu_descriptions:
    otu_dict[row[0]] = row[1]