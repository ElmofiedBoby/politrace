import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from git import Repo
from config import Config
from graph import SourceMeta

"""
Parser for legislator data from github.com/unitedstates/congress-legislators.
"""

logging.basicConfig(level=logging.INFO)


class LegislatorParser:
    """
    Handles downloading, organizing, and parsing legislator data from the
    `unitedstates/congress-legislators` GitHub repository.
    """

    def __init__(self, config: Optional[Config] = None) -> None:
        self.config: Config = config or Config()
        self.legislator_url: str = "https://github.com/unitedstates/congress-legislators.git"
        self.legislator_raw_path: Path = Path(self.config.raw_data_path, "legislators")
        self.legislator_clean_path: Path = Path(self.config.clean_data_path, "legislators")
        self.legislators_current_path: Path = self.legislator_clean_path / "legislators-current.yaml"
        self.legislators: List[Dict[str, Any]] = []

        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("LegislatorParser initialized.")

    def load(self) -> None:
        """Public method to load and process legislator data."""
        self._load_data()
        self._load_legislators()
        if self.legislators:
            self.logger.info(f"First legislator loaded: {self.legislators[0]}")

    def get_source(self) -> SourceMeta:
       return SourceMeta(
            source_id="congress-legislators@sample",
            url="https://github.com/unitedstates/congress-legislators",
            publisher="unitedstates/congress-legislators",
        )
    
    def _load_legislators(self) -> None:
        """Load and process legislator YAML data."""
        self.logger.info("Loading legislators from YAML...")
        try:
            with self.legislators_current_path.open("r", encoding="utf-8") as file:
                legislators_data = yaml.safe_load(file) or []

            self.logger.info(f"Loaded {len(legislators_data)} legislators.")

            self.legislators = [
                self._process_legislator(legislator) for legislator in legislators_data
            ]

        except FileNotFoundError:
            self.logger.error(f"File not found: {self.legislators_current_path}")
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML: {e}")

    def _process_legislator(self, legislator: Dict[str, Any]) -> Dict[str, Any]:
        """Extract relevant fields from a legislator record."""
        bioguide_id = legislator.get("id", {}).get("bioguide")
        name = legislator.get("name", {})
        bio = legislator.get("bio", {})
        terms = legislator.get("terms", [])

        self.logger.debug(
            f"Processing legislator: {name.get('first')} {name.get('last')} (ID: {bioguide_id})"
        )

        return {
            "bioguide_id": bioguide_id,
            "name": name,
            "bio": bio,
            "terms": terms,
            "ids": legislator.get("id", {}),
            "current_term": terms[-1] if terms else None,
        }

    def _load_data(self) -> None:
      """Ensure legislator data is available locally."""
      
      def download_data() -> None:
        """Clone legislator data repository."""
        self.logger.info(f"Cloning data from {self.legislator_url}...")
        Repo.clone_from(self.legislator_url, self.legislator_raw_path)
        self.logger.info("Cloning complete.")

      def reorganize_data() -> None:
        """Move YAML files from raw path to clean path."""
        self.legislator_clean_path.mkdir(parents=True, exist_ok=True)

        for file in self.legislator_raw_path.iterdir():
          if file.suffix == ".yaml":
              target = self.legislator_clean_path / file.name
              self.logger.info(f"Moving {file.name} to clean path...")
              file.replace(target)

        self.logger.info("Reorganization complete.")
      
      if self.legislator_clean_path.exists() and any(self.legislator_clean_path.iterdir()):
        self.logger.info("Clean data already exists. Skipping download.")
      elif self.legislator_raw_path.exists() and any(self.legislator_raw_path.iterdir()):
        self.logger.info("Raw data already exists. Organizing files...")
        reorganize_data()
      else:
        self.logger.info("No data found. Downloading files...")
        download_data()
        self.logger.info("Download complete. Organizing files...")
        reorganize_data()
