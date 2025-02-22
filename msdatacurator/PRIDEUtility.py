from ftplib import FTP
import os
import gzip
import pickle
import shutil 

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
        self.dataset_paths = None  # Dictionary to store dataset paths

    
    def _load_dataset_paths(self):
        """
        Lazy-loads the dataset paths and creates the dataset_paths dictionary.
        """
        if self.dataset_paths is None:
            self.dataset_paths = {}
            with FTP(self.ftp_host) as ftp:
                try:
                    ftp.login()
                    ftp.cwd(self.ftp_path)
                    years = ftp.nlst()
                    for year in years:
                        if year == '2010':                            
                            year_path = f"{self.ftp_path}/{year}"
                            indices = ftp.nlst(year_path)
                            for index in indices:
                                index_path = index
                                datasets = ftp.nlst(index_path)
                                for dataset in datasets:
                                    dataset_path = dataset
                                    self.dataset_paths[os.path.basename(dataset)] = dataset_path
                except Exception as e:
                    print(f"Error listing datasets: {str(e)}")

    def list_datasets(self):
        """
        Lists available datasets and their full paths.

        Returns:
            dict: Dictionary mapping dataset identifiers to full paths.
        """
        self._load_dataset_paths()  # Lazy load dataset paths
        return self.dataset_paths

    def download_dataset(self, dataset_identifier, file_format="raw"):
        """
        Downloads all files of the specified dataset in the given file format.

        Args:
            dataset_identifier (str): The PX dataset identifier (e.g., PXD123456).
            file_format (str): Optional. The desired file format ("raw", "mgf", or "mgf.gz").

        Returns:
            bool: True if download is successful, False otherwise.
        """      
        dataset_path = self.dataset_paths.get(dataset_identifier)

        if not dataset_path:
            print(f"Dataset {dataset_identifier} not found.")
            return False

        local_directory = dataset_identifier
        os.makedirs(local_directory, exist_ok=True)  # Create local directory if not exists

        with FTP(self.ftp_host) as ftp:
            try:
                ftp.login()
                ftp.set_pasv(True)  # Use passive mode

                # Check if the "generated" subdirectory exists
                generated_subdirectory = os.path.join(dataset_path, "generated").replace("\\", "/")
                
                all_files = ftp.nlst(dataset_path)
                for file in all_files: 
                    if file.lower().endswith("generated"):
                        ftp.cwd(generated_subdirectory)
                        # List all files in the "generated" subdirectory                        
                        files = ftp.nlst()                
                    else:
                        # List all files in the main directory
                        files = all_files                                
            
                if file_format.lower() == "raw":
                    filtered_files = [f for f in files if f.lower().endswith(".raw")]
                elif file_format.lower() == "mgf":
                    filtered_files = [f for f in files if f.lower().endswith(".mgf")]
                elif file_format.lower() == "mgf.gz":
                    filtered_files = [f for f in files if f.lower().endswith(".mgf.gz")]                    
                else:
                    raise ValueError("Invalid file format. Choose 'raw', 'mgf', or 'mgf.gz'.")

                for remote_filename in filtered_files:
                    local_filename = os.path.join(local_directory, "generated", remote_filename) if "generated" in all_files else os.path.join(local_directory, remote_filename)
                    with open(local_filename, "wb") as local_file:
                        ftp.retrbinary(f"RETR {remote_filename}", local_file.write)

                    # Extract .mgf.gz files if needed
                    if file_format.lower() == "mgf.gz" and local_filename.endswith(".mgf.gz"):
                        with gzip.open(local_filename, 'rb') as f_in:
                            with open(local_filename[:-3], 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        os.remove(local_filename)  # Remove the .mgf.gz file after extraction

                return True
            except Exception as e:
                print(f"Error downloading dataset {dataset_identifier}: {str(e)}")
                return False

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
    dataset_paths = pride_utility.list_datasets()  # Populate dataset_paths dictionary

    # Save dataset_paths to a local file (e.g., pickle)
    with open("dataset_paths.pkl", "wb") as file:
        pickle.dump(dataset_paths, file)
    print("Dataset paths saved locally.")

    # Exemplary data set 
    dataset_id = "PRD000817"
    success = pride_utility.download_dataset(dataset_id, file_format="mgf.gz")
    if success:
        print(f"Dataset {dataset_id} downloaded successfully.")
    else:
        print(f"Failed to download dataset {dataset_id}.")
    
    # Write dataset list to a CSV file
    #csv_filename = "datasets_list.csv"
    #pride_utility.write_datasets_to_csv(datasets, csv_filename)
