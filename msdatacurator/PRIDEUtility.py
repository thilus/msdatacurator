from ftplib import FTP
import csv

class PRIDEUtility:
    def __init__(self, ftp_host, ftp_path):
        self.ftp_host = ftp_host
        self.ftp_path = ftp_path

    def list_years(self):
        with FTP(self.ftp_host) as ftp:
            ftp.login()
            ftp.cwd(self.ftp_path)
            return ftp.nlst()

    def list_indices(self, year_path):
        with FTP(self.ftp_host) as ftp:
            ftp.login()
            ftp.cwd(year_path)
            return ftp.nlst()

    def list_datasets(self, index_path):
        with FTP(self.ftp_host) as ftp:
            ftp.login()
            ftp.cwd(index_path)
            return ftp.nlst()

    def write_datasets_to_csv(self, datasets, csv_filename="datasets_list.csv"):
        with open(csv_filename, mode='w', newline='') as csv_file:
            fieldnames = ['Dataset']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            for dataset in datasets:
                writer.writerow({'Dataset': dataset})

        print(f"Dataset list has been written to {csv_filename}")

if __name__ == "__main__":
    ftp_host = "ftp.pride.ebi.ac.uk"
    ftp_path = "/pride/data/archive/"

    pride_utility = PRIDEUtility(ftp_host, ftp_path)

    years = pride_utility.list_years()
    datasets = []

    for year in years:
        year_path = f"{ftp_path}{year}/"
        indices = pride_utility.list_indices(year_path)

        for index in indices:
            index_path = f"{year_path}{index}/"
            datasets.extend(pride_utility.list_datasets(index_path))

    print("Available datasets:")
    for idx, dataset in enumerate(datasets, start=1):
        print(f"{idx}. {dataset}")

    csv_filename = "datasets_list.csv"
    pride_utility.write_datasets_to_csv(datasets, csv_filename)
