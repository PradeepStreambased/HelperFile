import json
from sqlalchemy.engine import create_engine

def convert_kafka_properties(kafka_properties: str) -> str:
    # Split input string into lines
    lines = kafka_properties.strip().split('\n')
    
    # Remove comments and empty lines
    no_comments = [line for line in lines if not line.strip().startswith("#") and line.strip()]
    
    
    # Map lines to key-value pairs
    mapped = [map_line(line) for line in no_comments]
    
    # Join mapped lines into the final format
    out = ",\n".join(mapped)
    
    # Create the final session setting string
    session_setting = f"{{\n {out}\n }}"
    return session_setting

def map_line(line: str) -> str:
    key = line.split('=', 1)[0].strip()
    value = line.split('=', 1)[1].strip()
    return f'  "{key}":"{value}"'

def create_kafka_engine(kafka_properties: str):
    # Convert the properties to session setting
    streambased_connection = convert_kafka_properties(kafka_properties)
    
    connect_args = {
        "session_properties": {"streambased_connection": streambased_connection},
        "http_scheme": "https",
        "schema": "streambased"
    }
    
    # Create and return the engine
    engine = create_engine("trino://streambased.cloud:8443/kafka", connect_args=connect_args)
    
    return engine
