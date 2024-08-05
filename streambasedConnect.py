import json
from sqlalchemy.engine import create_engine

def create_kafka_engine(kafka_properties: str):
    # Parse the input string into a dictionary
    properties_dict = {}
    for line in kafka_properties.strip().split('\n'):
        if '=' in line:
            key, value = line.split('=', 1)
            properties_dict[key.strip()] = value.strip()
    
    # Extract required properties
    bootstrap_servers = properties_dict.get('bootstrap.servers')
    security_protocol = properties_dict.get('security.protocol')
    sasl_jaas_config = properties_dict.get('sasl.jaas.config')
    sasl_mechanism = properties_dict.get('sasl.mechanism')
    schema_registry_url = properties_dict.get('schema.registry.url')
    basic_auth_credentials_source = properties_dict.get('basic.auth.credentials.source')
    basic_auth_user_info = properties_dict.get('basic.auth.user.info')

    # Create the connection string
    session_properties = {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": security_protocol,
        "sasl.jaas.config": sasl_jaas_config,
        "sasl.mechanism": sasl_mechanism,
        "schema.registry.url": schema_registry_url,
        "basic.auth.credentials.source": basic_auth_credentials_source,
        "basic.auth.user.info": basic_auth_user_info
    }
    
    streambased_connection = json.dumps(session_properties)
    
    connect_args = {
        "session_properties": {"streambased_connection": streambased_connection},
        "http_scheme": "https",
        "schema": "streambased"
    }
    
    # Create and return the engine
    engine = create_engine("trino://streambased.cloud:8443/kafka", connect_args=connect_args)
    return engine
