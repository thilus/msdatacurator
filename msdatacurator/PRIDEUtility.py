from ftplib import FTP
import csv

class PRIDEUtility:
    def __init__(self, ftp_host, ftp_path):
        """
        Initializes the PRIDEUtility class.

        Args:
            ftp_host (str): The FTP server hostname.
            ftp_path (str): The base path on the FTP server.
        """
        self.ftp_host = ftp_host
        self.ftp_path = ftp_path

    def list_years(self):
        """
        Connects to the FTP server and lists available years (subdirectories).

        Returns:
            list: A list of available years.
        """
        with FTP(self.ftp_host) as ftp:
            ftp.login()
            ftp.cwd(self.ftp_path)
            return ftp.nlst()

    def list_indices(self, year_path):
        """
        Lists available indices (subdirectories) within a given year.

        Args:
            year_path (str): The path to the year directory.

        Returns:
            list: A list of available indices.
        """
        with FTP(self.ftp_host) as ftp:
            ftp.login()
            ftp.cwd(year_path)
            return ftp.nlst()

    def list_datasets(self, index_path):
        """
        Lists available datasets (subdirectories) within a given index.

        Args:
            index_path (str): The path to the index directory.

        Returns:
            list: A list of available datasets.
        """
        with FTP(self.ftp_host) as ftp:
            ftp.login()
            ftp.cwd(index_path)
            return ftp.nlst()

    def write_datasets_to_csv(self, datasets, csv_filename="datasets_list.csv"):
        """
        Writes the list of datasets to a CSV file.

        Args:
            datasets (list): List of dataset names.
            csv_filename (str, optional): Name of the CSV file. Defaults to "datasets_list.csv".
        """
        with open(csv_filename, mode='w', newline='') as csv_file:
            fieldnames = ['Dataset']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            for dataset in datasets:
                writer.writerow({'Dataset': dataset})

        print(f"Dataset list has been written to {csv_filename}")

if __name__ == "__main__":
    # Set FTP server details
    ftp_host = "ftp.pride.ebi.ac.uk"
    ftp_path = "/pride/data/archive/"

    # Create an instance of PRIDEUtility
    pride_utility = PRIDEUtility(ftp_host, ftp_path)

    # Get available years
    years = pride_utility.list_years()
    datasets = []

    # Iterate through years and indices to collect datasets
    for year in years:
        year_path = f"{ftp_path}{year}/"
        indices = pride_utility.list_indices(year_path)

        for index in indices:
            index_path = f"{year_path}{index}/"
            datasets.extend(pride_utility.list_datasets(index_path))

    # Print available datasets
    print("Available datasets:")
    for idx, dataset in enumerate(datasets, start=1):
        print(f"{idx}. {dataset}")

    # Write dataset list to a CSV file
    csv_filename = "datasets_list.csv"
    pride_utility.write_datasets_to_csv(datasets, csv_filename)
