# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os
import datetime
import logging
import numpy as np
import pandas as pd

from ops_core.mailer import MandrillMailer, parse_address
#from utils import *

ALERT_SUBJECT = "FishSOOP Processing Daily Update: {yesterday_date}"

BODY_STYLE = """
<style>
.logo {
    position: absolute;
}
img {
    display: block;
    margin: auto;
}
table td th {
    border: 1px solid black;
    margin: auto;
}
td {
    padding: 2px 20px;
}
</style>
"""


ALERT_BODY_HTML = """
<html>
<head>
{style}
</head>
<body>
<img style="float: right;" src={logo}>


<p>Yesterday's file processing statistics: </p>

<table>
    <thead><tr><th>Status</th></tr><thead>
    <tbody>
    <tr><td style="font-weight: bold;">Report Date: </td><td> {yesterday_date}</td></tr>
    <tr><td style="font-weight: bold;">Number of received files: </td><td>{num_infiles}</td></tr>
    <tr><td style="font-weight: bold;">Number of successfully processed files: </td><td>{num_success}</td></tr>
    <tr><td style="font-weight: bold;">Number of failed files: </td><td> {num_failed}</td></tr>
    <tr><td style="font-weight: bold;">Failed Moana Serial Numbers ([] if none): </td><td> {sns_failed}</td></tr>
    </tbody>
</table>
<p> A csv file of failed files is attached; if no files failed, no csv is attached.
If you have any questions or comments, please contact fishsoop@unsw.edu.au.  This
is an automatic email that is generated once per day (0 UTC).  The information contained in this
email message (including any attachments) is STRICTLY CONFIDENTIAL. If you are not the intended
recipient then please notify the sender immediately and then delete the e-mail.  Anyone other
than the intended recipient must not use, disclose, copy or distribute this message,
the information in it, or any attachments. </p>
</body>
</html>
"""

ALERT_BODY_TEXT = """
    Report Date: {yesterday_date}
    Number of received files: </td><td>{num_infiles}
    Number of successfully processed files:{num_success}
    Number of failed files: </td><td> {num_failed}
    Failed Moana Serial Numbers (nan if none): {sns_failed}
    A csv file of failed files is attached; if no files failed, no csv is attached.
    If you have any questions or comments, please contact fishsoop@unsw.edu.au.  This
    is an automatic email that is generated once per day (0 UTC).  The information contained in this
    email message (including any attachments) is STRICTLY CONFIDENTIAL. If you are not the intended
    recipient then please notify the sender immediately and then delete the e-mail.  Anyone other
    than the intended recipient must not use, disclose, copy or distribute this message,
    the information in it, or any attachments.
"""


class QCStatusMailer(object):
    """
    Sends an email with qc status_file csv attached (filtered version)
    and some summary stats, intended for once per day.
    Inputs:
        csv: status file for the day
    Output:
        Email is send and status file is updated.  Nothing is returned.
    """

    def __init__(self,
                 from_email='fishsoop@unsw.edu.au',
                 bcc=[],
                 status_file='s3://fishsoop-moana-qc1/status-files/status_file_%y%m%d.csv',
                 outfile='s3://fishsoop-email/failed_files_%y%m%d.csv',
                 recipients=False,
                 reply_to=None,
                 logo='fsoop_logo.png',
                 default_email=['fishsoop@unsw.edu.au'],
                 logger=logging,
                 pipe=None,
                 **kwargs):
        self.logger.info('In monitoring.py: __init__')
        self.filename = status_file
        self.outfile = outfile
        self.from_email = from_email
        self.recipients = recipients
        self.bcc = bcc
        self.reply_to = reply_to
        self.logo = logo
        self.default_email = default_email
        self.logger = logger
        self.mailer = MandrillMailer()
        self.pipe = pipe
        self.logger = logger

    def set_cycle(self, cycle_dt):
        self.logger.info('In monitoring.py: set_cycle')
        self.cycle_dt = cycle_dt
        self.report_date = (datetime.datetime.now()
                            - datetime.timedelta(days=1))

    def _process_status_file(self):
        self.logger.info('In monitoring.py: _process_status_file')
        filename = self.report_date.strftime(self.filename)
        df = pd.read_csv(filename)
        df = df[df['filename'].str.contains("MOANA_")]
        df2 = df[df['failed'] == 'yes']
        self.outfile = self.report_date.strftime(self.outfile)
        if len(df2) > 0:
            df2.to_csv(self.outfile)
            return(df, True)
        else:
            return(df, False)

    def _get_context(self, df):
        """
        Calculate values needed for email subject and body, to fill in
        fields where needed.  Some of these come from the netCDF file
        attributes.
        """
        self.logger.info('In monitoring.py: _get_context')
        # Below, get_plots is not used for anything right now
        context = {
             'logo': self.logo,
             'style': BODY_STYLE,
        }
        context['yesterday_date'] = self.report_date.strftime('%d %b %y')
        context['num_failed'] = len(df[df['failed'] == 'yes'])
        context['num_success'] = len(df[df['saved'] == 'yes'])
        context['num_infiles'] = len(df)
        context['sns_failed'] = str(
            np.unique(df[df['failed'] == 'yes']['moana_serial_number']))

        return context

    # def _calc_infiles(self):
    #     incoming_files = []
    #     indir = os.path.join(self.newfile_dir, self.file_format)
    #     self.logger.info(f'Looking for new files in {indir}...')
    #     files_in_dir = glob.glob(indir, recursive=True)
    #     start_date = self.cycle_dt - timedelta(days=1)
    #     for filename in files_in_dir:
    #         filetime = dt.datetime.fromtimestamp(os.path.getmtime(filename))
    #         if (filetime > start_date) and (filetime <= self.cycle_dt):
    #             incoming_files.append(filename)
    #     return len(incoming_files)

    def has_been_alerted(self):
        """
        Check if email has been sent
        """
        self.logger.info('In monitoring.py: has_been_alerted')
        now = datetime.datetime.utcnow()
        params = {
            'query': 'subject:%s' % self.report_date.strftime('%d %b %y'),
            'senders': [parse_address(self.from_email)['email']],
            'date_from': (now-datetime.timedelta(hours=24)).date().isoformat(),
            'limit': 1
        }

        last_email = self.mailer.mandrill.messages.search(**params)

        if last_email:
            return True
        else:
            return False

    def _get_email_parameters(self):
        self.logger.info('In monitoring.py: _get_email_parameters')
        df, anyfailed = self._process_status_file()
        context = self._get_context(df)
        if not self.recipients:
            self.logger.info('No recipients found')
            self.recipients = self.default_email
        self.logger.info(
            f'Sending Mangopare data email to {len(self.recipients)} recipients...')
        self.subject = ALERT_SUBJECT.format(**context)
        self.html = ALERT_BODY_HTML.format(**context)
        self.text = ALERT_BODY_TEXT.format(**context)
        if anyfailed:
            self.all_attach = [self.outfile]
        else:
            self.all_attach = []

    def run(self):
        """
        Send email with data, check if email sent, record sent email
        """
        self.logger.info('In monitoring.py: run')
        try:
            #import ipdb
            #ipdb.set_trace()
            self._get_email_parameters()
            self.logger.info(
                f'Emailing {self.recipients} Mangopare data...')
            self.mailer.send_email(to=self.recipients, bcc=self.bcc,
                                   subject=self.subject,
                                   html=self.html,
                                   text=self.text,
                                   from_=self.from_email,
                                   reply_to=self.reply_to,
                                   attachments=self.all_attach,
                                   important=False)
            good_email = self.has_been_alerted()
            os.remove(self.outfile)
            if good_email:
                self.logger.info('Email sent.')
            else:
                self.logger.info('Email not sent, something went wrong.')
        except Exception as exc:
            self.logger.error(
                f'Mangopare QC status email not sent due to {exc}')
