import os

# Database connection details
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'rick_and_morty_db')
DB_USER = os.getenv('DB_USER', 'user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
DB_PORT = os.getenv('DB_PORT', '5432')

# API details
RICK_AND_MORTY_API_URL = "https://rickandmortyapi.com/api/character"

# Table Names
BRONZE_TABLE_NAME = 'bronze.bronze_characters'
SILVER_TABLE_NAME = 'silver.silver_characters'
GOLD_TABLE_NAME = 'gold.gold_character_names' # As per README, gold_character_names, not gold_characters
