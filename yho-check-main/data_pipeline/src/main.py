from data_pipeline.src.mongo_extractor import extract_and_load_mongo_data
from data_pipeline.src.postgres_extractor import extract_and_load_postgres_data

def main():
    """Main function to run the data pipeline."""
    print("Starting data pipeline run...")
    extract_and_load_mongo_data()
    extract_and_load_postgres_data()
    print("Data pipeline run finished.")

if __name__ == "__main__":
    main()
