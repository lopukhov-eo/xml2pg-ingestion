import configparser
from pathlib import Path

config_path = Path(__file__).resolve().parent.parent.parent / "config.ini"

config = configparser.ConfigParser()
config.read(config_path)

XML_TAG_NAME = config["XML"].get("name_tag")
XML_GROUP_TAG_NAME = config["XML"].get("group_tag_name")
