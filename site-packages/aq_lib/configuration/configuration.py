from configparser import ConfigParser
import os


class Configuration:
    def __init__(self, config_path):
        self.config = ConfigParser()
        self.config.read(config_path)

    def get_property(self, section, key):
        if os.getenv(key, None):
            return os.getenv(key, None)
        try:
            return self.config[section][key]
        except:
            return None



