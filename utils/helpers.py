import datetime
import json
import os

import click

CONFIG_PATH = 'config.json'


def load_config():
    """Load configuration data from config.json"""
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
    """Save configuration data to config.json"""
    try:
        with open(CONFIG_PATH, 'w') as config_file:
            json.dump(config_data, config_file, indent=4)
    except Exception as e:
        click.echo(f"Error saving configuration: {e}")
        exit(1)


def generate_filename(query):
    """Generates a simple filename based on the table name, first few words of the query, and current date."""

    current_date = datetime.datetime.now().strftime("%Y%m%d")
    words = query.lower().split()

    # Find the table name
    try:
        table_index = words.index('from') + 1
        table = words[table_index].split('.')[-1]
    except (ValueError, IndexError):
        table = 'unknown'

    # Get the first three words after 'select'
    try:
        select_index = words.index('select')
        action = '_'.join(words[select_index + 1:select_index + 4])
        action = action.replace('*', 'all').replace(',', '')
    except (ValueError, IndexError):
        action = 'unknown'

    # Combine date, table and action into a filename of up to 50 characters
    filename = f"{current_date}_{table}_{action[:50]}"

    # Remove any non-alphanumeric characters (except underscore and period)
    filename = ''.join(c for c in filename if c.isalnum() or c in ['_', '.'])

    return filename
