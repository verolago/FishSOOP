import re
import logging
import boto3
from ops_mangopare.utils import import_pycallable
from ops_mangopare.plot import PlotMangopare
from ops_mangopare.mails import MangopareMailer

class SendDataWrapper(object):
    """
    plot and/or email data to specified email address that has been
    processed by ops_qc.wrappper.  Creates one plot and sends one email
    per file in filelist.
    Input:
        filelist: list of quality-controlled netCDF files to plot and/or
        email.  These should be produced using the metocean/ops_qc repository
        or similar.
        email_to: list of email addresses to send all of the files to.  If
        this is None (default), the code looks for a vessel_email attribute
        in the xarray dataset resulting from reading the file in filelist.
        Each email is then sent to the vessel_email specified in the ds
        attributes.
        email_from: email address that appears in the "from" field
        email_reply_to: list of email addresses that will be emailed
        if the recipient replies
        bcc_emails: list of bcc emails for all emails corresponding to
        the files in filelist
        status_file: path to the status file used in mails.py - see
        mails.MangopareMailer for more information
        cutoff_num: files with this number of measurements or fewer will not be
        sent.  Filters sensors that were just splashed.
        create_status_file: if no status file already exists, create it (again,
        see mails.MangopareMailer)
        logo_file: path and filename of logo to include in plot, if needed
        email_plot: whether to include the plot in the email
        email_raw_data: whether to include the processed csv in the email (a
        bit of a misleading name...)
        plot_out_dir: specify a directory where plots are saved.  If no out_dir,
        plots are saved in same directory as files in filelist
        plot_add_map: whether to include a map in the plot.  With current
        plot design, if this is False, there will be an empty white space
        where the map would go.
        plot_data: whether to create a plot of the data
        datareader: python class to read the qc'd netCDF files
    Output:
        Plots, csv files, and emails are created as specified.
        Nothing is returned.
    """

    def __init__(self,
                 filelist=None,
                 email_to=None,
                 email_from='fishsoop@unsw.edu.au',
                 email_reply_to=['fishsoop@unsw.edu.au'],
                 bcc_emails=['fishsoop@unsw.edu.au'],
                 status_file='s3://fishsoop-email/fishsoop_emails_sent.csv',
                 cutoff_num=5,
                 create_status_file=False,
                 logo_file='fsoop_logo.png',
                 email_plot=True,
                 email_raw_data=True,
                 plot_out_dir=False,
                 plot_add_map=True,
                 plot_data=True,
                 datareader={},
                 logger=logging,
                 pipe=None,
                 **kwargs):

        self.filelist = filelist
        self.email_to = email_to
        self.email_from = email_from
        self.email_reply_to = email_reply_to
        self.bcc_emails = bcc_emails
        self.status_file = status_file
        self.cutoff_num = cutoff_num
        self.create_status_file = create_status_file
        self.logo_file = logo_file
        self.email_plot = email_plot
        self.email_raw_data = email_raw_data
        self.plot_out_dir = plot_out_dir
        self.plot_add_map = plot_add_map
        self.plot_data = plot_data
        self.datareader = datareader
        self._default_datareader = 'ops_mangopare.readers.MangopareNetCDFReader'
        self.plot_metadata = []
        self.default_email_to = ['fishsoop@unsw.edu.au']
        self.pipe = pipe
        self.logger = logger

    def set_cycle(self, cycle_dt):
        '''
        This function will be called by the scheduler after instantiation,
        passing a datetime object, in UTC, representing the current Cycle.
        That's how Scheduler tells to your process at which cycle to run.
        '''
        self.logger.error('In wrapper.py: set_cycle')
        self.cycle = cycle_dt

    def _set_filelist(self):
        """
        If self._success_file exists, use to send emails
        """
        self.logger.error('In wrapper.py: _set_filelist')
        if hasattr(self, 'success_files') and not self.filelist:
            self.filelist = self._success_files
        if not self.filelist:
            self.logger.error(
                'No file list found, please specify.  No QC performed.')

    def _set_class(self, in_class, default_class):
        self.logger.error('In wrapper.py: _set_class')
        klass = in_class.pop('class', default_class)
        out_class = import_pycallable(klass)
        self.logger.error('Using class: %s ' % klass)
        return(out_class)

    def _get_email_addresses(self):
        """
        Get to and from email addresses
        """
        self.logger.error('In wrapper.py: _get_email_addresses')
        email_pattern = re.compile(
            "(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
        if not self.email_to:
            try:
                ds_email_to = self.ds.attrs['vessel_email'].split(",")
                ds_email_to = [email.strip() for email in ds_email_to]
                self.email_to_final = []
                [self.email_to_final.append(
                    email) for email in ds_email_to if email_pattern.match(email)]
                
                self.logger.error(f'email addresses to send to: {self.email_to_final}')    
            except Exception as exc:
                self.logger.error(
                    'No to email found, using default email address')
                self.email_to_final = self.default_email_to
        else:
            self.email_to_final = self.email_to

    def _set_all_classes(self):
        self.logger.error('In wrapper.py: _set_all_classes')
        try:
            self.datareader = self._set_class(
                self.datareader, self._default_datareader)
        except Exception as exc:
            self.logger.error(
                'Unable to set required classes to read data: {}'.format(exc))
            raise exc
        
    def _set_logo_file(self):
        self.logger.error('In wrapper.py: _set_logo_file')
        try:
            if self.ds.attrs['programme_name'] == 'Fish-Soop':
                self.logo_file = 'fsoop_logo.png'
                self.sensor_name = 'Moana'
        except:
            pass

    def run(self):
        self.logger.error('In wrapper.py: run')
        plot_list = []
        self._set_filelist()
        self._set_all_classes()
        self.logger.info(
            f'Attemping to send emails for the following files: {self.filelist}')
        for filename in self.filelist:
            try:
                if self.email_raw_data:
                    save_csv = True
                else:
                    save_csv = False
                self.ds, csv_file = self.datareader(
                    filename, save_csv=save_csv,logger=self.logger).run()
                    
                if not (self.ds.attrs['email_frequency'] == 'nrt' and self.ds.attrs['email_status'] == 'on'):
                    continue
                # don't email if not enough data (i.e. filter out splashed sensors)
                if len(self.ds.DATETIME.values) <= self.cutoff_num:
                    continue
                self._set_logo_file()
                if self.plot_data:
                    plot_file, time_vals = PlotMangopare(
                        ds=self.ds, filename=filename, logo_file=self.logo_file, 
                        out_dir=self.plot_out_dir, add_map=self.plot_add_map, 
                        logger=self.logger).run()
                if self.email_raw_data and self.plot_data:
                    plot_list = [plot_file]
                    self.logger.error(f'In wrapper.py: run, adding plot: {plot_list}')
                self._get_email_addresses()
                if self.email_plot or self.email_raw_data:
                    MangopareMailer(ds=self.ds,
                                    plots=plot_list,
                                    from_email=self.email_from,
                                    bcc=self.bcc_emails,
                                    additional_attachments=csv_file,
                                    recipients=self.email_to_final,
                                    reply_to=self.email_reply_to,
                                    status_file=self.status_file,
                                    create_status_file=self.create_status_file,
                                    logger=self.logger).run()
                self.ds.close()
            except Exception as exc:
                self.logger.error(
                    f'Send email failed for {filename} due to {exc}.')
