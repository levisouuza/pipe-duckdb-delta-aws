from model.config_variables import ConfigVariables
from processor.ingestion_processor import IngestionProcessor


def main():
    config = ConfigVariables()  # noqa
    ingestion_processor = IngestionProcessor(config)
    ingestion_processor.process()


if __name__ == "__main__":
    main()