# Databricks notebook source
# MAGIC %pip install pyfiglet

# COMMAND ----------
from pyfiglet import Figlet

user_text = input('Enter text to convert to ASCII art: ')
figlet = Figlet()
print(figlet.renderText(user_text))