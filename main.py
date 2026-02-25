from src.validation_config.read_config import ReadFile
from src.analysis.analysis import Analysis
from src.get_frame import get_frame
from typing import List, Any

config= ReadFile().read_file()
file= config.data.input_path

frame= get_frame(file=file)

Analysis(frame=frame, config=config).run_analysis()

def data() -> List[Any]: 
    return [frame, file]

















