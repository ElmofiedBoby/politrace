from pydantic import BaseModel
from pathlib import Path


class Config(BaseModel):
  raw_data_path: Path = Path("./data/raw")
  clean_data_path: Path = Path("./data/clean")
