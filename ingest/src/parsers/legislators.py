from config import Config
from pathlib import Path
from git import Repo
from os import listdir, replace
import logging

"""
Parsers legislator data from github.com/unitedstates/congress-legislators.
"""

logging.basicConfig(level=logging.INFO)

class LegislatorParser:

  config: Config = Config()
  legislator_url: str = "https://github.com/unitedstates/congress-legislators.git"
  legislator_raw_path: Path = Path(config.raw_data_path, "legislators")
  legislator_clean_path: Path = Path(config.clean_data_path, "legislators")
  logger: logging.Logger = logging.getLogger(__name__)

  def __init__(self):
    self.logger.info("Initializing LegislatorParser...")
    self._load_data()
    self.legislators_current_path: Path = Path(self.legislator_clean_path, "legislators-current.yaml")
    self._load_legislators()
    self.logger.info("Initialized!")

  def _load_legislators(self):
    self.logger.info("Loading legislators...")
    import yaml
    
    try:
      with open(self.legislators_current_path, 'r') as file:
        legislators_data = yaml.safe_load(file)
        
      self.logger.info(f"Loaded {len(legislators_data)} legislators")
      
      # Process each legislator
      for legislator in legislators_data:
        self._process_legislator(legislator)
        
    except FileNotFoundError:
      self.logger.error(f"File not found: {self.legislators_current_path}")
    except yaml.YAMLError as e:
      self.logger.error(f"Error parsing YAML: {e}")
  
  def _process_legislator(self, legislator):
    """Process individual legislator data"""
    # Extract basic info
    bioguide_id = legislator.get('id', {}).get('bioguide')
    name = legislator.get('name', {})
    bio = legislator.get('bio', {})
    terms = legislator.get('terms', [])
    
    self.logger.debug(f"Processing legislator: {name.get('first')} {name.get('last')} (ID: {bioguide_id})")
    
    # You can add your specific processing logic here
    # For example, storing to database, transforming data, etc.
    
    return {
      'bioguide_id': bioguide_id,
      'name': name,
      'bio': bio,
      'terms': terms,
      'ids': legislator.get('id', {}),
      'current_term': terms[-1] if terms else None
    }
    
  
  def _load_data(self):
    if self.legislator_clean_path.exists() and len(listdir(self.legislator_clean_path)) > 0:
      self.logger.info("Data already exists! Skipping download.")
    elif self.legislator_raw_path.exists() and len(listdir(self.legislator_raw_path)) > 0:
      self.logger.info(
          "Data already downloaded! Skipping download and organizing...")
      self._reorganize_data()
    else:
      self._download_data()
      self._reorganize_data()
  
  def _download_data(self):
    self.logger.info(
        f"Cloning data {self.legislator_url} to {self.legislator_raw_path}.")
    Repo.clone_from(self.legislator_url, self.legislator_raw_path)
    self.logger.info("Cloning complete!")

  def _reorganize_data(self):
    Path.mkdir(Path.cwd() / self.legislator_clean_path, exist_ok=True)
    files = listdir(Path.cwd() / self.legislator_raw_path)
    for file in files:
      if file.endswith(".yaml"):
        self.logger.info(f"Reorganizing {file}...")
        replace(self.legislator_raw_path / file,
                self.legislator_clean_path / file)
    self.logger.info("Reorganizing complete!")
