import psycopg2
import logging
from flask import Flask, request, jsonify
from flask_restful import Api
from pip._internal.network import session
from sqlalchemy import Column, String, Integer, Date, BOOLEAN, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

logging.basicConfig(level="DEBUG")

app = Flask(__name__)
api = Api(app)

Base = declarative_base()
database_url = "postgres://fqushgifgxjxng:b1ebbb383a151c16caf63dfdcce5d79c20879122bf36fde1d64f74fc777d0eaa@ec2-52-6-75-198.compute-1.amazonaws.com:5432/d39md9jiu6q4pl"

# disable sqlalchemy pool using nullpool as by default postgress has its own pool
engine = create_engine(database_url, echo=True, poolclass=NullPool)

conn = engine.connect()

class EmpDetails(Base):
    __tablename__ = 'empdetails'
    Name = Column("name", String)
    Maritalstatus = Column("maritalstatus", String)
    Empid = Column("empid", String,primary_key=True)
    Salary = Column("salary", String)
    Bankname = Column("bankname", String)
    Bankaccountno = Column("bankaccountno", Integer)
    Panno = Column("panno", String)
    Ifsccode = Column("ifsccode", String)
    Departmentname = Column("departmentname", String)
    Subdepartmentname = Column("subdepartmentname", String)
    Age = Column("age",Integer)
    Dob = Column("dob",Date)


class PersonalInfo(Base):
    __tablename__ = 'personalinfo'
    Name = Column("name", String)
    Age = Column("age", Integer)
    Maritalstatus = Column("maritalstatus", String)
    Empid = Column("empid", String, primary_key=True)
    DOB = Column("dob", Date)

Session = sessionmaker(bind=engine)
session = Session()

@app.route('/GETempdetails', methods=['GET'])
def home():
    Emp_id = request.args.get("Empid")
    result = session.query(EmpDetails).filter(EmpDetails.Empid == Emp_id).all()
    result = [item.__dict__ for item in result]
    for item in result:
        item.pop('_sa_instance_state')
    return str(result)

@app.route('/empdetails', methods=['GET'])
def home1():

    result = session.query(EmpDetails).order_by(EmpDetails.Dob).all()
    result = [item.__dict__ for item in result]
    for item in result:
        item.pop('_sa_instance_state')
    return str(result)

@app.route('/postpersonalinfo', methods=['POST'])
def postpersonalinfo():
    request_body = request.get_json(force=True)
    for index,item in enumerate(request_body):
        record = PersonalInfo(Name = item["name"],
                                     Age = item["age"],
                                     DOB = item["dob"],
                                     Empid =item["empid"])
        session.add_all([record])
    session.commit()
    return ("***data inserted in PersonalInfo table successfully***")



if __name__ == "__main__":
    app.run(debug=True)


