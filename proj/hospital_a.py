from flask import Flask, Response, request
from flask_sqlalchemy import SQLAlchemy #import SQLAlchemy because it allows us to easily do databases via python classes
from sqlalchemy.exc import IntegrityError #import exception class for checking database contraints
from sqlalchemy.engine import Engine #Needed to check for example, connections,
from sqlalchemy import event #and then we can listen for a event, like connection event via engine, and then we run a function after connection event
from datetime import datetime #import datetime for storing last update time for patients, and also for printing it in a nice format
from flask_restful import Api, Resource #import flask_restful, which allows us to easily build REST APIs via python classes. Api is the main API object, and Resource is the base class for defining API endpoints
from werkzeug.exceptions import NotFound #import NotFound exception for handling 404 errors when patient is not found
from werkzeug.routing import BaseConverter #import BaseConverter for defining custom URL converters, if needed

import os
import pika
import json
import threading
import time  

app = Flask(__name__) #Creates Flask application object, and tells flask that the application is located in app.py
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///hospital_a.db" #Tells flask to which database to connect to, and store database in file called "test.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False #Turns of object modification tracking 
db = SQLAlchemy(app) #Creates a SQLAlchemy database object
api = Api(app)
HOSPITAL_ID = os.getenv("HOSPITAL_ID", "hospital_a")


class Patient(db.Model):    #Database class for Artist object. Makes Artist tables
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    age = db.Column(db.Integer, nullable=False)
    diagnosis_TEXT = db.Column(db.String, nullable=False)
    last_update = db.Column(db.DateTime, nullable=False, default=datetime.now) #Store the last update time for the patient, and set default to now when creating a new patient  

    def __repr__(self): #Just more clear print output when python prints artists, not needed
        return f"[Patient {self.name}]"

class PatientCollection(Resource): #Defines the API endpoint for /patients, and the methods that it accepts (GET, POST)

    def get(self):
        patient_list = []
        patients = Patient.query.all() #Query all patients from database
        for pat in patients:
            patient_data = {
                "name": pat.name,
                "age": pat.age,
                "diagnosis_TEXT": pat.diagnosis_TEXT,
                "last_update": pat.last_update.strftime("%Y-%m-%d %H:%M:%S") #Format datetime nicely as string
            }
            patient_list.append(patient_data)
        return patient_list

    def post(self):
        if not request.is_json: #Check if request content type is JSON, if not return error
            return "Request content type must be JSON", 415
        incoming_JSON = request.json #Get the JSON data from the request
        fields = ["name", "age", "diagnosis_TEXT"] #Required fields for creating a new patient
        if not all(field in incoming_JSON for field in fields): #Check if all required fields are present, if not return error
            return "Incomplete request - missing fields", 400
        try:
            name = incoming_JSON["name"]
            age = int(incoming_JSON["age"]) #Try to convert age to integer, if it fails then we have a bad request
            diagnosis_TEXT = incoming_JSON["diagnosis_TEXT"]
        except (ValueError, TypeError):
            return "Invalid data types for fields", 400
        new_patient = Patient(name=name, age=age, diagnosis_TEXT=diagnosis_TEXT) #Create new patient object with the provided data
        db.session.add(new_patient) #Add new patient to database session
        try:
            db.session.commit() #Try to commit the session to save the new patient to the database
        except IntegrityError: #If there is an integrity error (like unique constraint violation), then we rollback the session and return an error
            db.session.rollback()
            return "Database integrity error - possible duplicate entry", 400
        event = {
            "action": "add_or_update",
            "id": new_patient.id,
            "name": new_patient.name,
            "age": new_patient.age,
            "diagnosis_TEXT": new_patient.diagnosis_TEXT,
            "last_update": new_patient.last_update.isoformat(),
            "origin": HOSPITAL_ID
        }
        send_to_broker(event) #Send the event to the message broker
        return f"Patient {name} created successfully", 201
    
    def delete(self):
        num_deleted = Patient.query.delete() #Delete all patients from the database, and get the number of deleted records
        db.session.commit() #Commit the session to save the changes to the database
        event = {
            "action": "delete_all",
            "id": None,
            "name": None,
            "age": None,
            "diagnosis_TEXT": None,
            "last_update": None,
            "origin": HOSPITAL_ID
        }
        send_to_broker(event) #Send the event to the message broker
        return f"Deleted {num_deleted} patients", 200
    
    
    
    
class PatientItem(Resource): #Defines the API endpoint for /patients/<id>, and the methods that it accepts (GET, PUT, DELETE

    def get(self, id):
        patient = Patient.query.get(id) #Query the patient with the given id from the database
        if not patient: #If patient does not exist, return error
            return "Patient not found", 404
        patient_data = {
            "name": patient.name,
            "age": patient.age,
            "diagnosis_TEXT": patient.diagnosis_TEXT,
            "last_update": patient.last_update.strftime("%Y-%m-%d %H:%M:%S")
        }
        return patient_data

    def put(self, id):
        patient = Patient.query.get(id) #Query the patient with the given id from the database
        if not patient: #If patient does not exist, return error
            return "Patient not found", 404
        if not request.is_json: #Check if request content type is JSON, if not return error
            return "Request content type must be JSON", 415
        incoming_JSON = request.json #Get the JSON data from the request
        fields = ["name", "age", "diagnosis_TEXT"] #Required fields for updating a patient
        if not all(field in incoming_JSON for field in fields): #Check if all required fields are present, if not return error
            return "Incomplete request - missing fields", 400
        try:
            patient.name = incoming_JSON["name"]
            patient.age = int(incoming_JSON["age"]) #Try to convert age to integer, if it fails then we have a bad request
            patient.diagnosis_TEXT = incoming_JSON["diagnosis_TEXT"]
            patient.last_update = datetime.now() #Update last update time to now, since we are modifying the patient
        except (ValueError, TypeError):
            return "Invalid data types for fields", 400
        try:
            db.session.commit() #Try to commit the session to save the updated patient to the database
        except IntegrityError: #If there is an integrity error (like unique constraint violation), then we rollback the session and return an error
            db.session.rollback()
            return "Database integrity error - possible duplicate entry", 400
        event = {
            "action": "add_or_update",
            "id": patient.id,
            "name": patient.name,
            "age": patient.age,
            "diagnosis_TEXT": patient.diagnosis_TEXT,
            "last_update": patient.last_update.isoformat(),
            "origin": HOSPITAL_ID
        }
        send_to_broker(event) #Send the event to the message broker
        return f"Patient {patient.name} updated successfully", 200
    
    def delete(self, id):
        patient = Patient.query.get(id) #Query the patient with the given id from the database
        if not patient: #If patient does not exist, return error
            return "Patient not found", 404
        db.session.delete(patient) #Delete the patient from the database session
        db.session.commit() #Commit the session to save the changes to the database
        event = {
            "action": "delete",
            "id": patient.id,
            "name": patient.name,
            "age": patient.age,
            "diagnosis_TEXT": patient.diagnosis_TEXT,
            "last_update": patient.last_update.isoformat()  ,
            "origin": HOSPITAL_ID
        }
        send_to_broker(event) #Send the event to the message broker 
        return f"Patient {patient.name} deleted successfully", 200


def send_to_broker(event_data):
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost", 51574, '/',pika.PlainCredentials('guest', 'guest')))
    channel = connection.channel() #Create a channel for communication with the broker
    channel.exchange_declare(exchange='patients_exchange', exchange_type='fanout', durable=True) 
    channel.basic_publish(exchange='patients_exchange', routing_key='', body=json.dumps(event_data), properties=pika.BasicProperties(delivery_mode=2))
    connection.close() #Close the connection to the broker

def start_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost", 51574, '/',pika.PlainCredentials('guest', 'guest')))
    channel = connection.channel()
    channel.exchange_declare(exchange='patients_exchange', exchange_type='fanout', durable=True)
    queue_name = f'patients_{HOSPITAL_ID}'
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange='patients_exchange', queue=queue_name)
    
    

    def callback(ch, method, properties, body):
        data = json.loads(body)
        # Skip own messages
        if data["origin"] == HOSPITAL_ID:
            return
        print("Received event:", data)
        with app.app_context():
            if data["action"] == "add_or_update":
                patient = Patient.query.get(data["id"])
                if patient:
                    patient.name = data["name"]
                    patient.age = data["age"]
                    patient.diagnosis_TEXT = data["diagnosis_TEXT"]
                    patient.last_update = datetime.fromisoformat(data["last_update"])
                else:
                    patient = Patient(
                        id=data["id"],
                        name=data["name"],
                        age=data["age"],
                        diagnosis_TEXT=data["diagnosis_TEXT"],
                        last_update=datetime.fromisoformat(data["last_update"])
                    )
                    db.session.add(patient)
                    db.session.commit()
            elif data["action"] == "delete":
                patient = Patient.query.get(data["id"])
                if patient:
                    db.session.delete(patient)
                    db.session.commit()
            elif data["action"] == "delete_all":
                Patient.query.delete()
                db.session.commit()
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    print("Consumer started...")
    channel.start_consuming()

api.add_resource(PatientCollection, "/patients") #Add the PatientCollection resource to the API at the endpoint /patients
api.add_resource(PatientItem, "/patients/<int:id>") #Add the PatientItem resource to the API at the endpoint /patients/<id>

if __name__ == "__main__":
    with app.app_context():
        db.create_all() #Creates the database tables based on the defined models (in this case, the Patient model)
    threading.Thread(target=start_consumer, daemon=True).start()
    time.sleep(5)  # give consumer time to connect
    app.run(debug=True, port=5000) #Runs the Flask application in debug mode, which provides helpful error messages and auto-reloads the server when code changes are detected
                