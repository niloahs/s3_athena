"""
Author: Mark Mekhail
Date: 10/13/24
Description: This module provides helper functions for configuration management,
filename generation, and determining the default S3 bucket based on file type.
"""

import datetime
import json
import mimetypes
import os

import click

CONFIG_PATH = 'config.json'


def load_config():
    """
    Load configuration data from config.json.

    Raises:
        SystemExit: If the configuration file is not found or cannot be loaded.
    """
    if not os.path.exists(CONFIG_PATH):
        click.echo(
            "Configuration file 'config.json' not found. Please run the setup command first.")
        exit(1)
    try:
        with open(CONFIG_PATH, 'r') as config_file:
            return json.load(config_file)
    except json.JSONDecodeError as e:
        click.echo(f"Error parsing 'config.json': {e}")
        exit(1)
    except Exception as e:
        click.echo(f"Error loading configuration: {e}")
        exit(1)


def save_config(config_data):
    """
    Save configuration data to config.json.

    Args:
        config_data (dict): The configuration data to save.

    Raises:
        SystemExit: If the configuration data cannot be saved.
    """
    try:
        with open(CONFIG_PATH, 'w') as config_file:
            json.dump(config_data, config_file, indent=4)
    except Exception as e:
        click.echo(f"Error saving configuration: {e}")
        exit(1)


def generate_filename(query):
    """
    Generates a simple filename based on the table name, first few words of the query, and current date.

    Args:
        query (str): The SQL query to generate the filename from.

    Returns:
        str: The generated filename.
    """
    current_date = datetime.datetime.now().strftime("%Y%m%d")
    words = query.lower().split()

    try:
        table_index = words.index('from') + 1
        table = words[table_index].split('.')[-1]
    except (ValueError, IndexError):
        table = 'unknown'

    try:
        select_index = words.index('select')
        action = '_'.join(words[select_index + 1:select_index + 4])
        action = action.replace('*', 'all').replace(',', '')
    except (ValueError, IndexError):
        action = 'unknown'

    filename = f"{current_date}_{table}_{action[:50]}"
    filename = ''.join(c for c in filename if c.isalnum() or c in ['_', '.'])

    return filename


def get_default_bucket(filename, config_data):
    """
    Determine the appropriate bucket based on file type.

    Args:
        filename (str): The name of the file.
        config_data (dict): The configuration data containing bucket names.

    Returns:
        str: The name of the appropriate bucket.
    """
    _, file_extension = os.path.splitext(filename.lower())
    mime_type, _ = mimetypes.guess_type(filename)

    if mime_type and mime_type.startswith('image/'):
        return config_data['images_bucket']
    elif file_extension in ['.csv', '.txt', '.json']:
        return config_data['data_bucket']
    else:
        return config_data['data_bucket']  # Default to data bucket if file type is unknown
