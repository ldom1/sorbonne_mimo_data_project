from data_project.api.get_data import get_data
from data_project.core.analysis.map import generate_paris_map
from data_project.core.config import PARIS_MAP_PATH
from data_project.core.processing.main import process_data


def main():
    # Get data
    get_data()
    # Process data
    process_data()
    generate_paris_map(output_path=PARIS_MAP_PATH)


if __name__ == "__main__":
    main()
