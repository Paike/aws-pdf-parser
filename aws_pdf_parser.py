import json
import re
import boto3
import os
import fitz
import logging
import logging.config
import sys
import csv
import traceback
import datetime
import time
from dateutil.parser import parse
import base64
from email import message_from_string
import yaml


#  gets triggered by SES email saved in S3
def dispatch_email_attachments(event, context):

    if 'AWS_EXECUTION_ENV' not in os.environ: 
        delete_logs()

    configuration_bucket = "configuration_bucket"
    logger = Logger(__name__).get_logger()

    if 'AWS_EXECUTION_ENV' not in os.environ:    
        with open('event_sns_message.json', 'r') as file:
            json_data = file.read()
        sns_message = json.loads(json_data)
    else:
        sns_message = json.loads(event['Records'][0]['Sns']['Message'])

    headers = sns_message['mail']['commonHeaders']
    inbound_emails_bucket = sns_message['receipt']['action']['bucketName']
    object_key = sns_message['receipt']['action']['objectKey']


    logger.debug("Inbound bucket: %s", inbound_emails_bucket)
    logger.debug("Email object key: %s", object_key)

    to_address = headers['to'][0]

    pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
    match = re.search(pattern, to_address)

    if match:
        to_address = match.group()
    else:
        logger.critical("No email address found in %s", to_address)
        return False

    from_address = headers['from'][0]

    pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
    match = re.search(pattern, from_address)
    if match:
        from_address = match.group()

    logger.info("Receiving email from %s to %s", from_address, to_address)

    config_id = to_address.split('@')[0]
    if not config_id.isalnum():
        error_message = "Configuration id has wrong format: %s" % config_id
        logger.critical(error_message)
        raise ValueError(error_message)

    if 'AWS_EXECUTION_ENV' not in os.environ:  
        config_id = "xxxxxxxx"

    logger.debug("Configuration id: %s", config_id)

    config = Configuration(configuration_bucket, config_id)
    if not config:
        raise ValueError("Empty config")

    s3_client = boto3.client('s3')
    raw_email = s3_client.get_object(Bucket=inbound_emails_bucket, Key=object_key)
    encoded_email_content = raw_email['Body'].read()
    decoded_email_content = encoded_email_content.decode('utf-8')

    email_message = message_from_string(decoded_email_content)

    if email_message.is_multipart():
        for part in email_message.get_payload():
            if part.get_content_type() == 'application/pdf':
                attachment_filename = part.get_filename()
                attachment_data = base64.b64decode(part.get_payload())
                with open("/tmp/" + attachment_filename, 'wb') as file:
                    file.write(attachment_data)
                
                if 'AWS_EXECUTION_ENV' in os.environ:  
                    document = Document("/tmp/" + attachment_filename, config)

                if 'AWS_EXECUTION_ENV' not in os.environ:  
                    document = Document("files/aef_invoice2.pdf", config)
            
                parser = Parser(document, config)
                logger.debug("Parser: %s", parser)
                if parser.elements is None:
                    logger.info("No elements found in file %s ", attachment_filename)
                    continue
                exporter = Exporter(document, parser, config)
                exporter.export()

    else:
        logger.info("Configuration id: %s", config_id)


# gets triggered by S3 file creation
def parse_pdf_file(event, context):
    try:
        document = Document(event)
        if not document.text:
            raise ValueError("Empty document")

        logger = Logger(__name__).get_logger()

        configuration_bucket = "configuration_bucket"
        output_bucket = "output_bucket"

        config = Configuration(configuration_bucket, document.identifier)
        if not config:
            raise ValueError("Empty config")

        logger.debug("Document text: %s", document.text)
        logger.debug("Document identifier: %s", document.identifier)
        logger.debug("Document filepath: %s", document.filepath)

        parser = Parser(config.elements, document.text)
        logger.debug("Elements: %s", parser.elements)

        exporter = Exporter(document, parser.elements, config.configuration)
        exporter.set_bucket(output_bucket)
        exporter.set_filename_base("data")
        exporter.export("csv")
        exporter.export("json")
        exporter.set_filename_base("data_rows")
        exporter.export("json_rows")

        return True

    except Exception as e:
        error_message = f"An error occurred during PDF parsing: {str(e)}"
        logger = Logger(__name__).get_logger()
        logger.error(error_message)
        traceback_message = traceback.format_exc()
        full_error_message = f"{error_message}\n\n{traceback_message}"
        logger.debug(full_error_message)


def delete_logs():
    logger = Logger(__name__).get_logger()    
    
    folder_path = "logs"

    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            if filename.endswith(".log"):
                file_path = os.path.join(folder_path, filename)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        logger.debug(f"{filename} has been deleted.")
                    elif os.path.isdir(file_path):
                        logger.debug(f"{filename} is a directory and will not be deleted.")
                except Exception as e:
                    logger.debug(f"Error deleting {filename}: {e}")
    else:
        logger.debug("The specified folder does not exist.")


class Parser:
    def __init__(self, document, config):
        self.logger = Logger(__class__.__name__).get_logger()
        
        self.constants = {}
        self.elements = self.process_elements(config.tasks[document.identifier]['parsing'], document.text)

        self.logger.debug("Elements: %s", self.elements)
        self.logger.debug("Constants: %s", self.constants)


    def get_iso_8601_date(self, date_string):
        parsed_date = parse(date_string)
        formatted_datetime = parsed_date.strftime("%Y-%m-%d")
        return formatted_datetime

    def check_constants(self, element, value):
        if 'constant' in element:            
            name = element.get('name')
            self.constants[f"{{{name}}}"] = value

    def process_element(self, element, text):
        pattern = element.get('pattern')
        mode = element.get('mode')

        required = element.get('required', False)

        self.logger.info('Processing element %s with pattern %s',
                         element["name"], element["pattern"])

        result = {
            element['name']: {
                'value': None,
                'children': []
            }
        }

        self.logger.debug("===== Start processing =====")
        self.logger.debug("Name: %s", element["name"])
        self.logger.debug("Column Type: %s", element["column_type"])
        self.logger.debug("Pattern: %s", element["pattern"])

        if mode == 'match':
            match = re.search(pattern, text, flags=re.UNICODE)
            if match:
                value = element.get('value', '')
                result[element['name']]['value'] = value
                self.check_constants(element, value)
                self.logger.debug("Found match")
                self.logger.info("Found match")
            else:
                if required:
                    error_message = f"The '{element['name']}' is required with match but does not exist"
                    self.logger.error(error_message)
                    raise ValueError(error_message)
                else:
                    message = f"The '{element['name']}' was not found with match"
                    self.logger.debug(message)
                    self.logger.info(message)
        elif mode == 'find_once':
            match = re.search(pattern, text, flags=re.UNICODE)
            if match:
                value = match.group(1)
                if element["column_type"] == "DATE":
                    value = self.get_iso_8601_date(value)
                result[element['name']]['value'] = value
                self.check_constants(element, value)                
                self.logger.debug("Found %s with find_once",
                                  result[element['name']]['value'])
                self.logger.info("Found %s with find_once",
                                 result[element['name']]['value'])
            else:
                if required:
                    error_message = f"The '{element['name']}' is required with find_once but does not exist"
                    self.logger.error(error_message)
                    raise ValueError(error_message)
                else:
                    message = f"The '{element['name']}' was not found with find_once"
                    self.logger.debug(message)
                    self.logger.info(message)
        elif mode == 'find_all':
            matches = re.findall(pattern, text, flags=re.UNICODE)
            if matches:
                self.logger.debug("matched: %s", matches)

                for i, match in enumerate(matches):
                    self.logger.debug("Found %s with find_all", match)
                    self.logger.info("Found %s with find_all", match)
                    child_elements = []
                    for child in element.get('children', []):
                        self.logger.debug(
                            "Parser process_element child: %s", child)
                        child_element = self.process_element(child, match)
                        child_elements.append(child_element)
                    child_result = {
                        f"{element['name']}_{i}": {
                            'value': match,
                            'children': child_elements
                        }
                    }
                    result[element['name']]['children'].append(child_result)
            else:
                if required:
                    error_message = f"The '{element['name']}' is required with find_all but does not exist"
                    self.logger.error(error_message)
                    raise ValueError(error_message) 
                else:
                    message = f"The '{element['name']}' was not found with find_all"
                    self.logger.debug(message)
                    self.logger.info(message)
        else:
            self.logger.debug("Wrong mode setting")
            self.logger.error("Wrong mode setting")
            self.logger.info("Wrong mode setting")

        return result

    def process_elements(self, elements, text):
        try:
            processed_elements = []
            for element in elements:
                processed_element = self.process_element(element, text)
                processed_elements.append(processed_element)
        except Exception as e:
            self.logger.error(f"Error processing element: {element}. Exception: {e}")
            return None

        return processed_elements


class Configuration:
    def __init__(self, configuration_bucket, configuration_id):
        self.logger = Logger(__class__.__name__).get_logger()

        
        self.configuration_bucket = configuration_bucket
        self.logger.debug("Configuration bucket: %s", self.configuration_bucket)  
        
        self.configuration_id = configuration_id
        self.logger.debug("Project id: %s", self.configuration_id)
         
        self.filepath = self.get_configuration_filepath()
        self.logger.debug("Filepath: %s", self.filepath)

        data = self.load_configuration_yaml(self.filepath)
        
        self.tasks = data["tasks"]
        self.logger.debug("Tasks: %s", self.tasks)        

        self.keywords = data["keywords"]
        self.logger.debug("Keywords: %s", self.keywords)

        self.export = data["export"]
        self.export_bucket = self.export["s3"]["bucket"]
        self.logger.debug("Export bucket: %s", self.export_bucket)              
     
        self.export_filename_base = self.export["s3"]["filename_base"]

    def load_configuration_yaml(self, filepath):
        with open(filepath, "r") as yaml_file:
            data = yaml.safe_load(yaml_file)
        if 'AWS_EXECUTION_ENV' in os.environ:            
            os.remove(filepath)
            self.logger.debug("Deleted configuration file: %s", filepath)  
        self.logger.debug("Configuration: %s", data)    
        self.logger.debug("Tasks: %s", data['tasks'])    
        return data

    def get_configuration_filepath(self):
        if 'AWS_EXECUTION_ENV' in os.environ:
            s3_client = boto3.client('s3')
            filepath = f"/tmp/configuration.{self.configuration_id}.yml"
            self.logger.debug("Configuration temp filepath: %s", filepath)
            s3_client.download_file(
                self.configuration_bucket, f"configuration.{self.configuration_id}.yml", filepath)
        else:
            filepath = f"configuration/configuration.{self.configuration_id}.yml"
        return filepath

    def load_json_file(self, filepath):
        try:
            with open(filepath, 'r') as json_file:
                data = json.load(json_file)

                if not data:
                    self.logger.error(f"The JSON file '{filepath}' is empty")
                    raise ValueError(f"The JSON file '{filepath}' is empty")

                required_keys = ["configuration", "elements"]
                for key in required_keys:
                    if key not in data:
                        self.logger.error(
                            f"Key '{key}' missing in JSON file '{filepath}'")
                        raise KeyError(
                            f"Key '{key}' missing in JSON file '{filepath}'")
                return data

        except FileNotFoundError:
            self.logger.error(f"Cannot find '{filepath}'")
            raise FileNotFoundError(f"Cannot find '{filepath}'")

        except json.JSONDecodeError:
            self.logger.error(f"Cannot decode '{filepath}'")
            raise json.JSONDecodeError(f"Cannot decode '{filepath}'")

        except KeyError as e:
            self.logger.error(str(e))
            raise KeyError(str(e))

        except Exception as e:
            self.logger.error(f"An error occurred: {str(e)}")
            raise Exception(f"An error occurred: {str(e)}")


class Logger:
    def __init__(self, name):
        log_config = {
            'debug': {
                'log_file_name': 'logs/debug.log',
                'log_file_formatter': '%(asctime)s - %(lineno)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
            },
            'error': {
                'log_file_name': 'logs/error.log',
                'log_file_formatter': '%(asctime)s - %(lineno)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s'
            },
            'info': {
                'log_file_name': 'logs/info.log',
                'log_file_formatter': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            }
        }
        self.logger = logging.getLogger(name)
        self.logger.handlers.clear()
        self.logger.setLevel(logging.DEBUG)
        if not 'AWS_EXECUTION_ENV' in os.environ:
            self.set_file_handler(log_config)

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(log_config['error']['log_file_formatter'])

        stream_handler.setLevel(logging.ERROR)
        self.logger.addHandler(stream_handler)

    def set_file_handler(self, log_config):
        for level, config in log_config.items():
            log_file_name = config['log_file_name']
            log_file_formatter = config['log_file_formatter']

            file_handler = logging.FileHandler(log_file_name)
            formatter = logging.Formatter(log_file_formatter)

            file_handler.setFormatter(formatter)
            file_handler.setLevel(logging.getLevelName(level.upper()))

            self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger


class Document:
    def __init__(self, filepath, configuration):
        self.logger = Logger(__class__.__name__).get_logger()

        try:
            self.filepath = filepath
            self.text = self.get_text_from_pdf()

            self.identifier = self.get_document_group_by_dict(configuration.keywords)

        except Exception as e:
            error_message = f"An error occurred while initializing Document: {str(e)}"
            self.logger.error(error_message)
            raise Exception(error_message)

    def set_filepath(self, filepath):
        self.filepath = filepath
        self.logger.debug("Set filepath to: %s", self.filepath)

    def set_identifier(self, identifier):
        self.identifier = identifier

    def get_document_group_identifier(self, filepath):
        filename = os.path.basename(filepath)
        name_parts = filename.split('_')
        if len(name_parts) < 1:
            error_message = f"Invalid filename: {filename}"
            self.logger.error(error_message)
            raise ValueError(error_message)
        identifier = name_parts[0]
        return identifier

    def get_document_group_by_dict(self, dict):
        search_terms = list(dict.keys())
        for search_term in search_terms:
            if search_term in self.text:
                document_group = dict[search_term]
                self.logger.debug("Document group: %s", document_group)
                return document_group
        self.logger.debug("Could not find document group")
        return None

    def get_text_from_pdf(self):
        self.logger.debug("Processing PDF: %s" % self.filepath)
        self.logger.debug("========== PROCESSING PDF ============")
        self.logger.debug("processing %s" % self.filepath)

        try:
            doc = fitz.open(self.filepath)
            all_blocks = ""

            for page in doc:
                blocks = page.get_text("text")
                all_blocks += blocks
            doc.close()

            text = all_blocks

            if 'AWS_EXECUTION_ENV' in os.environ:
                os.remove(self.filepath)
            self.text = text
            self.logger.debug("Text %s" % text)
            return text

        except Exception as e:
            error_message = f"An error occurred while processing PDF: {str(e)}"
            self.logger.error(error_message)
            raise Exception(error_message)


class Exporter:
    def __init__(self, document, parser, configuration):
        self.logger = Logger(__class__.__name__).get_logger()

        self.document = document
        self.elements = parser.elements
        self.constants = parser.constants

        self.constants["{id}"] = document.identifier
                       
        self.configuration = configuration.tasks[document.identifier]['export']
        
        self.export_formats = configuration.export["s3"]["formats"]
        self.export_bucket = configuration.export["s3"]["bucket"]      
        self.export_filename_base = configuration.export["s3"]["filename_base"]

        self.set_filename_base(self.constants)

        self.directory = ""
        self.delimiter = ";"    
 
    def set_bucket(self, export_bucket):
        self.export_bucket = export_bucket
        self.logger.debug("Set bucket to: %s", self.export_bucket)

    def set_directory(self, directory):
        if not directory.endswith(os.path.sep):
            directory = os.path.join(directory, "")
        self.directory = directory
        self.logger.debug("Set directory to: %s", self.directory)

    def set_filename_base(self, constants):
        
        self.logger.debug("Constants: %s", constants)
        self.logger.debug("Constants type: %s", type(constants))
        
        match = re.search(r'{%[YmdHMSbB%-]*}', self.export_filename_base)
        if match:
            self.logger.debug("Matched date format: %s", match.group())
            current_datetime = datetime.datetime.now()
            formatted_datetime = current_datetime.strftime(match.group()).replace("{","").replace("}","")
            self.logger.debug("New datetime: %s", formatted_datetime)
            self.export_filename_base = self.export_filename_base.replace(match.group(), formatted_datetime)         

            self.logger.debug("Set filename base to: %s", self.export_filename_base)

        for placeholder, value in constants.items():
            self.logger.debug("Placeholder: %s Value: %s", placeholder, value)
            self.logger.debug("Filename format: %s", self.export_filename_base)
            self.export_filename_base = self.export_filename_base.replace(placeholder, value)
            self.logger.debug("Set filename base to: %s", self.export_filename_base) 

    def set_delimiter(self, delimiter):
        if len(delimiter) > 1:
            raise ValueError("Delimiter should be a single character.")
        self.logger.debug("Set delimiter to: %s", self.delimiter)            
        self.delimiter = delimiter

    def export(self):
        self.logger.debug("Export type set to: %s", self.export_formats)
        for format in self.export_formats:     
            if format == 'json':
                self.export_to_json(self.elements)
            elif format == 'csv':
                self.export_to_csv(self.elements)
            elif format == 'json_rows':
                self.export_to_json_as_table(self.elements)
            else:
                raise ValueError(
                    "Unsupported export type. Only 'csv', 'json', and 'json_rows' are supported.")

    def export_to_json(self, elements):
        self.write_file(elements, self.export_filename_base + ".full.json")

    def export_to_csv(self, elements):
        data = self.get_table_array(elements)
        self.logger.debug("data: %s", data["data"][0].keys())
        fieldnames = data["data"][0].keys()
        self.write_file(data["data"],  self.export_filename_base + ".csv", fieldnames)

    def export_to_json_as_table(self, elements):

        data = self.get_table_array(elements)

        self.write_file(data, self.export_filename_base + ".rows.json")

    def get_table_array(self, elements):
        row_source = self.configuration[0]['row_source']
        constant_columns = self.configuration[0]['constant_columns']

        constant_columns_dict = {}
        for constant_column in constant_columns:
            for element in elements:
                if constant_column in element:
                    value = element[constant_column]['value']
                    self.logger.debug(value)
                    constant_columns_dict[constant_column] = value

        json_data = []
        for item in elements:
            if row_source in item:
                positions = item[row_source]["children"]
                for i, position in enumerate(positions):
                    row = {
                        "row_number": i + 1,
                        "identifier": self.document.identifier,
                    }
                    row.update(constant_columns_dict)

                    self.logger.debug("position: %s" % position)
                    for key, item in position.items():
                        self.logger.debug("position items: %s" % value)
                        if "children" in item:
                            children = item["children"]
                            self.logger.debug("Children: %s" % children)
                            for child in children:
                                for key, value in child.items():
                                    row[key] = value["value"]
                                    self.logger.debug(
                                        "Child value: %s" % value)
                    json_data.append(row)

        return {"data": json_data}

    def write_file(self, data, filename, fieldnames=None, delimiter=";"):
        self.logger.debug("Data: %s", data)   
        filename = self.directory + filename
        self.logger.debug("Filename: %s", filename)   
        file_extension = filename.split('.')[-1].lower()

        if 'AWS_EXECUTION_ENV' not in os.environ:
            if file_extension == 'json':
                with open(f"output/{filename}", 'w') as jsonfile:
                    json.dump(data, jsonfile, indent=4)
                self.logger.debug("Saved JSON file: %s", filename)
            elif file_extension == 'csv':
                if fieldnames is None:
                    fieldnames = data[0].keys()

                with open(f"output/{filename}", 'w', newline='') as csvfile:
                    writer = csv.DictWriter(
                        csvfile, fieldnames=fieldnames, delimiter=delimiter)
                    writer.writeheader()
                    writer.writerows(data)
                self.logger.debug("Saved CSV file: %s", filename)
            else:
                raise ValueError(
                    "Unsupported file format. Only JSON and CSV are supported.")

        if 'AWS_EXECUTION_ENV' in os.environ:
            if file_extension == 'json':
                with open("/tmp/" + filename, 'w') as jsonfile:
                    json.dump(data, jsonfile, indent=4)
                self.logger.debug("Saved JSON file: /tmp/%s", filename)
            elif file_extension == 'csv':

                if fieldnames is None:
                    fieldnames = data[0].keys()
                
                with open("/tmp/" + filename, 'w', newline='') as csvfile:
                    writer = csv.DictWriter(
                        csvfile, fieldnames=fieldnames, delimiter=delimiter)
                    writer.writeheader()
                    writer.writerows(data)
                self.logger.debug("Saved CSV file: %s", filename)
            else:
                raise ValueError(
                    "Unsupported file format. Only JSON and CSV are supported.")

            try:
                s3 = boto3.client('s3')
                s3.upload_file("/tmp/" + filename, self.export_bucket, filename)
                self.logger.debug("Saved file %s to S3 bucket %s", filename, self.export_bucket)
                os.remove("/tmp/" + filename)
            except Exception as e:
                raise Exception(
                    "Failed to upload file to S3 bucket: " + str(e))
