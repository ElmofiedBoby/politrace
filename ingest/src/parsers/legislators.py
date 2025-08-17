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

#logging.basicConfig(level=logging.INFO)


class LegislatorParser:
    """
    Handles downloading, organizing, and parsing legislator data from the
    `unitedstates/congress-legislators` GitHub repository.
    """

    def __init__(self, config: Optional[Config] = None, include_current: bool = True, include_historical: bool = True) -> None:
        self.config: Config = config or Config()
        self.legislator_url: str = "https://github.com/unitedstates/congress-legislators.git"
        self.legislator_raw_path: Path = Path(self.config.raw_data_path, "legislators")
        self.legislator_clean_path: Path = Path(self.config.clean_data_path, "legislators")
        self.legislators_current_path: Path = self.legislator_clean_path / "legislators-current.yaml"
        self.legislators_historical_path: Path = self.legislator_clean_path / "legislators-historical.yaml"
        self.include_current: bool = include_current
        self.include_historical: bool = include_historical
        self.legislators: List[Dict[str, Any]] = []

        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("LegislatorParser initialized.")

    def load(self) -> None:
        """Public method to load and process legislator data."""
        self._load_data()
        self._load_legislators()

    def get_source(self) -> SourceMeta:
            return SourceMeta(
                source_id="congress-legislators@sample",
                url="https://github.com/unitedstates/congress-legislators",
                publisher="unitedstates/congress-legislators",
            )
    
    def _load_legislators(self) -> None:
        """Load and process legislator YAML data from current and/or historical files."""
        self.logger.info("Loading legislators from YAML...")
        loaded_count = 0
        combined: List[Dict[str, Any]] = []

        try:
            if self.include_current and self.legislators_current_path.exists():
                with self.legislators_current_path.open("r", encoding="utf-8") as f_cur:
                    current_data = yaml.safe_load(f_cur) or []
                self.logger.info(f"Loaded {len(current_data)} current legislators.")
                loaded_count += len(current_data)
                combined.extend([self._process_legislator(l, source_tag="current") for l in current_data])
            else:
                if self.include_current:
                    self.logger.warning(f"Requested current file but not found: {self.legislators_current_path}")

            if self.include_historical and self.legislators_historical_path.exists():
                with self.legislators_historical_path.open("r", encoding="utf-8") as f_hist:
                    hist_data = yaml.safe_load(f_hist) or []
                self.logger.info(f"Loaded {len(hist_data)} historical legislators.")
                loaded_count += len(hist_data)
                combined.extend([self._process_legislator(l, source_tag="historical") for l in hist_data])
            else:
                if self.include_historical:
                    self.logger.warning(f"Requested historical file but not found: {self.legislators_historical_path}")

            self.logger.info(f"Total legislators loaded: {loaded_count}")
            self.legislators = combined

        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML: {e}")

    def _process_legislator(self, legislator: Dict[str, Any], source_tag: str) -> Dict[str, Any]:
        """Extract relevant fields from a legislator record, tolerant of current vs historical schema."""
        # Normalize ids structure (`id` in most datasets, occasionally `ids`)
        ids: Dict[str, Any] = legislator.get("ids") or legislator.get("id") or {}
        bioguide_id = legislator.get("bioguide_id") or ids.get("bioguide") or ids.get("bioguide_id")

        name = legislator.get("name", {}) or {}
        bio = legislator.get("bio", {}) or {}
        terms = legislator.get("terms", []) or []

        self.logger.debug(
            f"Processing legislator: {name.get('first','')} {name.get('last','')} (ID: {bioguide_id}) [{source_tag}]"
        )

        return {
            "bioguide_id": bioguide_id,
            "name": name,
            "bio": bio,
            "terms": terms,
            "ids": ids,
            "current_term": terms[-1] if terms else None,
            "source_tag": source_tag,
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

        for file in self.legislator_raw_path.rglob("*.yaml"):
            # Only move the main legislator/committee YAMLs we actually use
            if file.name in {"legislators-current.yaml", "legislators-historical.yaml", "committees-current.yaml", "committees-historical.yaml"}:
                target = self.legislator_clean_path / file.name
                if target.exists():
                    continue
                self.logger.info(f"Moving {file} to clean path as {target.name}...")
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
