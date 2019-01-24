# --------------------------------------------------------------------------
# Dependencies
# --------------------------------------------------------------------------
from flask import Flask, jsonify, render_template
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, desc

# --------------------------------------------------------------------------
#  Initializes Flask
# --------------------------------------------------------------------------
app = Flask(__name__)

# --------------------------------------------------------------------------
# Initializes database connection
# --------------------------------------------------------------------------
engine = create_engine("sqlite:///DataSets/belly_button_biodiversity.sqlite")

Base = automap_base()
Base.prepare(engine, reflect=True)

Otu = Base.classes.otu
Samples = Base.classes.samples
Samples_metadata = Base.classes.samples_metadata

# --------------------------------------------------------------------------
# Creates session
# --------------------------------------------------------------------------
session = Session(engine)

# --------------------------------------------------------------------------
# Creates home route (renders HTML template)
# --------------------------------------------------------------------------
@app.route("/")
def home():
    return render_template("index.html")

# --------------------------------------------------------------------------
# Creates names route
# --------------------------------------------------------------------------
@app.route("/names")
def names():
    """ Function: Extracts names list from table
        Parameters: (none)
        Returns: JSON object of sample names """
    samples = Samples.__table__.columns
    samples_list = [sample.key for sample in samples]
    samples_list.remove("otu_id")
    return jsonify(samples_list)

# --------------------------------------------------------------------------
# Creates otu route
# --------------------------------------------------------------------------
@app.route("/otu")
def otu():
    """ Function: Extracts otu list from table
        Parameters: (none)
        Returns: JSON object of otu list """
    otu_descriptions = session.query(Otu.lowest_taxonomic_unit_found).all()
    otu_descriptions_list = [x for (x), in otu_descriptions]
    return jsonify(otu_descriptions_list)

# --------------------------------------------------------------------------
# Creates otu descriptions route
# --------------------------------------------------------------------------
@app.route("/otu_descriptions")
def otu_disc():
    """ Function: Extracts otu descriptions list from table
        Parameters: (none)
        Returns: JSON object of otu descriptions """
    otu_descriptions = session.query(Otu.otu_id, \
    Otu.lowest_taxonomic_unit_found).all()
    otu_dict = {}
    for row in otu_descriptions:
        otu_dict[row[0]] = row[1]
    return jsonify(otu_dict)

# --------------------------------------------------------------------------
# Creates metadata route for selected sample
# --------------------------------------------------------------------------
@app.route("/metadata/<sample>")
def sample_query(sample):
    """ Function: sample metadata query functionality
        Queries metadata table and extracts data for sample
        Parameters: 1 (string) sample ID
        Returns: JSON object of metadata for sample ID """
    sample_name = sample.replace("BB_", "")
    result = session.query(Samples_metadata.AGE, \
    Samples_metadata.BBTYPE, Samples_metadata.ETHNICITY, \
    Samples_metadata.GENDER, Samples_metadata.LOCATION, \
    Samples_metadata.SAMPLEID).filter_by(SAMPLEID = sample_name).all()
    record = result[0]
    record_dict = {
        "AGE": record[0],
        "BBTYPE": record[1],
        "ETHNICITY": record[2],
        "GENDER": record[3],
        "LOCATION": record[4],
        "SAMPLEID": record[5]
    }
    return jsonify(record_dict)

# --------------------------------------------------------------------------
# Creates wash frequency route for selected sample
# --------------------------------------------------------------------------
@app.route('/wfreq/<sample>')
def wash_freq(sample):
    """ Function: wash frequency query functionality
        Queries metadata table and extracts wash frequency
        Parameters: 1 (string) sample ID
        Returns: JSON object of wash frequency for sample ID """
    sample_name = sample.replace("BB_", "")
    result = session.query(Samples_metadata.WFREQ).\
    filter_by(SAMPLEID = sample_name).all()
    # Assignment specifies integer values (line 77); for sample BB_963
    wash_freq = round(result[0][0])
    return jsonify(wash_freq)

# --------------------------------------------------------------------------
# Creates samples route for selected sample
# --------------------------------------------------------------------------
@app.route('/samples/<sample>')
def otu_data(sample):
    """ Function: otu data query functionality
        Queries otu table and extracts data for sample
        Parameters: 1 (string) sample ID
        Returns: JSON object of otu data for sample ID """
    sample_query = "Samples." + sample
    result = session.query(Samples.otu_id, sample_query).\
    order_by(desc(sample_query)).all()
    otu_ids = [result[x][0] for x in range(len(result))]
    sample_values = [result[x][1] for x in range(len(result))]
    dict_list = [{"otu_ids": otu_ids}, {"sample_values": sample_values}]
    return jsonify(dict_list)

# --------------------------------------------------------------------------
# Executes Flask app
# --------------------------------------------------------------------------
if __name__ == '__main__':
    app.run(debug=True)
