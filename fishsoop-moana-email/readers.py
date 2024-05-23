import sys
import os
import re
import logging
import boto3
import xarray as xr

class MangopareNetCDFReader(object):
    """
    Read quality-controlled Mangopare temperature and
    pressure data in netCDF format for plotting
    and distribution to fishers.
    Input:
        filename: quality-controlled data in netCDF from obs_qc
        save_csv: whether to save a csv of the data in filename.
        The raw mangopare data comes in csv form, but this is
        different in that it is the qc'd data in a format intended
        for returning to the data owner (i.e. vessel).
        qc_keep: used to filter data for plot.py.  looks for the
        QC_FLAG variable and will only include data where QC_FLAG
        matches the values listed in qc_keep.  save_csv will include
        all values...this only filters for the plot.
        out_dir: where to save the csv file.  If none, it uses the
        directory that filename is in.
    Output:
        ds: xarray dataset with the data from filename
        csv_file: path and filename for the saved csv_file
    """

    def __init__(self,
                 filename,
                 save_csv=True,
                 qc_keep=[1, 2],
                 out_dir=None,
                 logger=logging):
        self.filename = filename
        self.save_csv = save_csv
        self.qc_keep = qc_keep
        self.out_dir = out_dir
        self.logger = logger

    def _read_netcdf(self):
        """
        Opens a qc'd netcdf mangopare file and only
        keep "good" data specified by qc_good
        """
        self.logger.error('In readers.py: _read_netcdf')
        try:
            # Extract the bucket name and object key from the S3 path
            bucket_name, object_key = self.filename.replace('s3://', '').split('/', 1)
            
            self.logger.error(f'In readers.py: reading file {object_key} from bucket {bucket_name}')
            
            # Initialize boto3 S3 client
            s3 = boto3.client('s3')

            # Download the file from S3
            local_file_path = '/tmp/' + os.path.basename(object_key)
            s3.download_file(bucket_name, object_key, local_file_path)

            # Open the downloaded file with xarray
            self.dsall = xr.open_dataset(local_file_path)

            good = [flag in self.qc_keep for flag in self.dsall.QC_FLAG.values]
            self.ds = self.dsall.where(good).dropna(dim='DATETIME')
            #self.logger.error(f'In readers.py: self.ds contents: {self.ds}')
        except Exception as exc:
            self.logger.error(
                'Could not read file {} due to {}'.format(self.filename, exc))
            raise exc

    def _save_ds_as_csv(self):
        """
        Kind of a strange place to put this, but it's
        here for now.  Can't really send fishers the
        original data since the positions are wrong.
        So we save the netcdf file as csv.  This saves
        all data, not just good data.
        """
        self.logger.error('In readers.py: _save_ds_as_csv')
        csv_header_keys = ['Moana serial number',
                           'Download time', 'Deck unit serial number',
                           'Moana calibration date', 'Moana Battery',
                           'Date quality controlled', 'Vessel Name', 'Vessel ID',
                           'Cellular upload position', 'Deck unit battery voltage']
        precision_values = {'LATITUDE': 6,
                            'LONGITUDE': 6, 'DEPTH': 1, 'TEMPERATURE': 2}
        df2 = self.dsall.to_dataframe()[
                                      ['LATITUDE', 'LONGITUDE', 'TEMPERATURE', 'DEPTH', 'QC_FLAG']].reset_index().copy()
        df2['DATETIME'] = [t.strftime('%Y-%m-%dT%H:%M:%S')
                           for t in df2.DATETIME]
        for name, prec in precision_values.items():
            df2[name] = df2[name].round(decimals=prec)
        df2 = df2.rename(columns={
                         'TEMPERATURE': 'TEMPERATURE [degC]', 'DEPTH': 'DEPTH [m]', 'DATETIME': 'DATETIME [UTC]'})
        # create csv header
        header_string = []
        for name in csv_header_keys:
            attrname = re.sub(" ", "_", name).lower()
            if hasattr(self.ds, attrname):
                header_string.append(f'{name}, {self.ds.attrs[attrname]}')
            else:
                header_string.append(f'{name}, NA')
        header_string.append(
            'QC_FLAG Key, 1=good 2=probably good 3=probably bad 4=bad')
        header_string.append('')
        header = '\n'.join(header_string)

        # Determine Moana serial number from the filename
        moana_serial_number = self.filename.split('/')[1]
        moana_serial_number_match = re.search(r'MOANA_(\d{4})_', self.filename)
        moana_serial_number = moana_serial_number_match.group(1)
        
        self.logger.error(f'In readers.py: Moana number: {moana_serial_number}')
        
        # Define CSV file path in /tmp directory
        csv_filename = os.path.splitext(os.path.basename(self.filename))[0] + '.csv'
        csv_file_path = '/tmp/' + csv_filename
        
        # Save DataFrame to CSV file in /tmp directory
        df2.to_csv(csv_file_path, header=True, index=False)
    
        # Initialize S3 client
        s3 = boto3.client('s3')
    
        # Upload CSV file to S3 bucket under the appropriate folder
        s3_bucket_name = 'fishsoop-email'
        s3_key = f'{moana_serial_number}/{csv_filename}'
        s3.upload_file(csv_file_path, s3_bucket_name, s3_key)
       
        return(csv_filename)
        
    def run(self):
        # read file based on self.filetype
        self.logger.error('In readers.py: run')
        try:
            self._read_netcdf()
            if self.save_csv:
                csv_file = self._save_ds_as_csv()
            else:
                csv_file = None
            
            return(self.ds, [csv_file])
            
        except Exception as exc:
            self.logger.error(
                f'Could not read or save csv for {self.filename}')
            return(None, [None])
