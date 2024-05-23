# -*- coding: utf-8 -*-
from __future__ import absolute_import
import os
import boto3
import datetime
import logging
import numpy as np
import pandas as pd
import smtplib
import tempfile
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import parseaddr

#from ops_core.mailer import MandrillMailer, parse_address

ALERT_SUBJECT = "{title_name} Serial Number {moana_serial_number} Temperature and Depth Data for {vessel_name} from {time_min} to {time_max}"

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

<table>
    <thead><tr><th>Vessel Information</th></tr><thead>
    <tbody>
    <tr><td style="font-weight: bold;">Vessel name: </td><td>{vessel_name}</td></tr>
    <tr><td style="font-weight: bold;">Time Range: </td><td> {time_min} - {time_max}</td></tr>
    <tr><td style="font-weight: bold;">Vessel email: </td><td> {vessel_email}</td></tr>
    </tbody>
</table>
<br/>
<table>
    <thead><tr><th>Data Summary</th></tr><thead>
    <tbody>
    <tr><td>  </td><td style="font-weight: bold;">Maximum</td><td style="font-weight: bold;">Minimum</td><td style="font-weight: bold;">Average</td></tr>
    <tr><td style="font-weight: bold;">Temperature</td><td>{temp_max}&#176;C</td><td>{temp_min}&#176;C</td><td>{temp_avg}&#176;C</td></tr>
    <tr><td style="font-weight: bold;">Depth</td><td>{depth_tmax} m</td><td>{depth_tmin} m</td><td>{depth_avg} m</td></tr>
    </tbody>
</table>
<p>Maximum depth of this sensor deployment: {depth_max} m</p>
<p> Note that depths in the Data Summary are the depths of the maximum and minimum temperature values, and the average depth of the deployment. </p>

<p>Please see attached the {sensor_name} temperature sensor data for {vessel_name}. Data quality-control and 
visualisation provided automatically by The Coastal and Regional Oceanography Lab at UNSW.</p>

<p>{data_statement}</p>

<p>If you have any questions or comments, please contact {contact_email}. This is an automatic email 
that is generated within 24 hours of data transmission from {vessel_name}.</p>

<p><small>The information contained in this email message (including any attachments) is STRICTLY CONFIDENTIAL. If you are not the 
intended recipient then please notify {contact_email} immediately and then delete the e-mail. Anyone other than the intended recipient must not use, 
disclose, copy or distribute this message, the information in it, or any attachments.</small></p>

</body>
</html>
"""

ALERT_BODY_TEXT = """
    Vessel name: {vessel_name}
    Time Range: {time_min} - {time_max}
    Deployment Temperature Summary:
    Temperature Range: {temp_min} degC - {temp_max} degC
    The mininum temperature occured at a depth of {depth_tmin} m and the maximum temperature at {depth_tmax} m.
    Temperature Average: {temp_avg} degC
    Depoyment Depth Summary:
    Maximum depth: {depth_max} m
    Depth Average: {depth_avg} m
    Note that depths in the Data Summary are the depths of the maximum and minimum temperature values, and the average depth of the deployment.
    Please see attached the {sensor_name} temperature sensor data for {vessel_name}. Data quality-control and visualisation provided automatically by MetOcean Solutions, a Division of the Meteorological Service of New Zealand.
    {data_statement} 
    If you have any questions or comments, please contact {contact_email}. This is an automatic email that is generated within 24 hours of data transmission from {vessel_name}.
    The information contained in this email message (including any attachments) is STRICTLY CONFIDENTIAL. If you are not the intended recipient then please notify {contact_email} immediately and then delete the e-mail. Anyone other than the intended recipient must not use, disclose, copy or distribute this message, the information in it, or any attachments.
    Errors: {email_error}
"""


class MangopareMailer(object):
    """
    Sends an email with attachments based on Moana netCDF attributes.  The scariest thing
    about this is that you could accidentally resend ALL the mangopare data using
    the ops-mangopare wrapper.  To avoid this (hopefully), ALWAYS USE A status_file
    VALUE!  DO NOT LEAVE AS NONE unless you really know what you are doing!
    if you are running this for the first time with a given status file, you can set
    create_status_file to True to make a new one.  It will fail if the file already exists.
    The email text/body is above.
    Inputs:
        ds: xarray dataset obtained from ops_mangopare/readers.py
        plots: list of plots to attach to email
        from_email: email address that the email will be sent from
        bcc: list of bcc email addresses
        additional attachments: list of any other files to attach
        recipients: list of email addresses to send email to.  If
        this is False, it will use the default_email.
        reply_to: email address to use if recipient replies to email
        status_file: keeps track of who has received emails of each
        plot in the past.  This is key to avoiding resending emails
        that have already been sent (i.e. if you rerun all the data
        and accidentally forget to shut off the email action).  I
        mostly trust this but not 100%.
        create_status_file: If a status file does not exist, it's
        a good idea to create one, for the above reason.
        logo: path/filename of a logo if you'd like to include it
        in the email.  Not sure it works right now.
        default_email: If recipients is False, the email will be
        sent to this address.
    Output:
        Email is send and status file is updated.  Nothing is returned.
    """

    def __init__(self, ds, plots,
                 from_email,
                 bcc=[],
                 additional_attachments=[],
                 recipients=False,
                 reply_to=None,
                 status_file='s3://fishsoop-email/fishsoop_emails_sent.csv',
                 create_status_file=False,
                 logo='fsoop_logo.png',
                 default_email=['fishsoop@unsw.edu.au'],
                 logger=logging):
        self.ds = ds
        self.plots = plots
        self.attachments = additional_attachments
        self.from_email = from_email
        self.recipients = recipients
        self.bcc = bcc
        self.reply_to = reply_to
        self.logo = logo
        self.status_file = status_file
        self.create_status_file = create_status_file
        self.default_email = default_email
        self.logger = logger
        #self.mailer = MandrillMailer()
        self.vessel_attrs = ['vessel_name','vessel_id','moana_serial_number','programme_name']
        self.default_email_text = {'sensor_name': 'Moana',
                                   'title_name': 'Moana',
                                   'contact_email': 'fishsoop@unsw.edu.au',
                                   'data_statement':'Moana sensor and deck unit provided by the FishSOOP Project.  The FishSOOP Project is funded by the FRDC and IMOS.'
        }

    def _get_context(self):
        """
        Calculate values needed for email subject and body, to fill in
        fields where needed.  Some of these come from the netCDF file
        attributes.
        """
        self.logger.error('In mails.py: _get_context')
        data_statement = self.default_email_text['data_statement']
        contact_email = self.default_email_text['contact_email']
        sensor_name = self.default_email_text['sensor_name']
        title_name = self.default_email_text['title_name']
        try:
            if self.ds.attrs['programme_name'] == 'Fish-Soop':
                data_statement = '''Temperature sensor and deck unit funded by the Integrated Marine Observing System (IMOS) 
                as part of Fisheries Research and Development Corporation (FRDC) project number 2022-07. 
                Data collected as part of FishSOOP: Oceanographic data collection on commercial fishing vessels; a partnership 
                between Fishwell Consulting (Fishwell) the University of New South Wales (UNSW) and the Integrated 
                Marine Observing System (IMOS). The project is co-funded by FRDC under project number 2022-07 and IMOS through 
                the Commonwealth Government's National Collaborative Research Infrastructure Strategy (NCRIS).'''
                contact_email = 'FishSOOP@unsw.edu.au'
                sensor_name = 'Moana'
                title_name = 'Moana'
                self.logo = 'fsoop_logo.png'
        except:
            pass
        # Below, get_plots is not used for anything right now
        context = {
             'logo': self.logo,
             'style': BODY_STYLE,
             'data_statement': data_statement,
             'contact_email': contact_email,
             'sensor_name': sensor_name,
             'title_name': title_name 
        }
        # vessel info
        for attr_name in self.vessel_attrs:
            if isinstance(self.ds.attrs[attr_name], (str, int)) and self.ds.attrs[attr_name] != 'NA':
                context[attr_name] = self.ds.attrs[attr_name]
            else:
                context[attr_name] = f'Unknown {attr_name}'
        #if not self.recipients:
        #    self.recipients = self.ds['Vessel Email'].split(",")
        context['time_min'] = str(
            np.min(self.ds.DATETIME.values).astype('datetime64[s]'))
        context['time_max'] = str(
            np.max(self.ds.DATETIME.values).astype('datetime64[s]'))
        # for testing only:
        context['vessel_email'] = self.ds.attrs['vessel_email']
        temp = self.ds.TEMPERATURE.values
        depth = self.ds.DEPTH.values
        i_tmin = np.nanargmin(temp)
        i_tmax = np.nanargmax(temp)
        context['temp_min'] = f'{temp[i_tmin]:.2f}'
        context['temp_max'] = f'{temp[i_tmax]:.2f}'
        context['temp_avg'] = f'{np.nanmean(temp):.2f}'
        context['depth_tmin'] = f'{depth[i_tmin]:.1f}'
        context['depth_tmax'] = f'{depth[i_tmax]:.1f}'
        context['depth_avg'] = f'{np.nanmean(depth):.1f}'
        context['depth_min'] = f'{np.nanmin(depth):.1f}'
        context['depth_max'] = f'{np.nanmax(depth):.1f}'
        context['email_error'] = 'None'
        return context

    def has_been_alerted(self):
        """
        Check if email has been sent
        """
        self.logger.error('In mails.py: has_been_alerted')
    
        # Define criteria for identifying the sent email
        sent_criteria = {
            'subject': self.subject,
            'recipient': self.recipients,  # Assuming recipients is a list
            'timestamp': datetime.datetime.utcnow()  # Current timestamp
        }
    
        try:
            # Attempt to check if an email meeting the criteria has been sent
            email_sent = self.check_sent_emails(sent_criteria)
        except Exception as e:
            self.logger.error(f"Error checking sent emails: {e}")
            email_sent = False  # Assume no email has been sent in case of error
    
        return email_sent

    def _l_to_s(self, listname):
        """
        convert list to string
        """
        self.logger.error('In mails.py: _l_to_s')
        return(', '.join(str(e) for e in listname))

    def _record_success(self):
        """
        Append email info to the status_file csv, or create csv if it doesn't exist yet
        """
        self.logger.error('In mails.py: _record_success')
        df = pd.DataFrame({'Datetime': datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'), 'Recipients': self._l_to_s(self.recipients), 'Attachments': self._l_to_s(self.attachments),
                           'Plots': self._l_to_s(self.plots), 'BCC': self._l_to_s(self.bcc), 'From': self._l_to_s([self.from_email]), 'Replyto': self._l_to_s([self.reply_to])}, index=[0])
        df.to_csv(self.status_file, mode='a', header=False)

    def _create_status_file(self):
        """
        If you choose to create status file, initializes csv
        """
        self.logger.error('In mails.py: _create_status_file')
        
        bucket_name, object_key = self.status_file.split('://')[1].split('/', 1)
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Check if the file already exists in the bucket
        try:
            s3_client.head_object(Bucket=bucket_name, Key=object_key)
            self.logger.error(
                f'Could not create status file because {self.status_file} already exists.')
        except s3_client.exceptions.ClientError as e:
            # If the file doesn't exist, create it
            if e.response['Error']['Code'] == '404':
                df = pd.DataFrame(columns=['Datetime', 'Recipients', 'Attachments', 'Plots', 'BCC', 'From', 'Replyto'])
                csv_buffer = df.to_csv(index=False)
                
                # Upload the DataFrame as CSV to the S3 bucket
                s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=csv_buffer)
            else:
                # If the error is not related to file not found, log and raise the exception
                self.logger.error(f'An error occurred: {e}')
                raise e

    def _get_email_parameters(self):
        self.logger.error('In mails.py: _get_email_parameters')
        context = self._get_context()
        if not self.recipients:
            self.logger.error('No recipients found')
            self.recipients = self.default_email
            context['email_error'] = 'No recipients found, sending to default email.'
        self.logger.error(
            f'Sending Moana data email to {len(self.recipients)} recipients...')
        self.subject = ALERT_SUBJECT.format(**context)
        self.html = ALERT_BODY_HTML.format(**context)
        self.text = ALERT_BODY_TEXT.format(**context)
        # Use this if attaching plots as regular attachments:
        self.all_attach = []
        self.all_attach.extend(self.attachments)
        self.all_attach.extend(self.plots)
        self.logger.error(f'In mails.py, get_email_param, self.plots is {self.plots} and all_attach is {self.all_attach}')
        self.all_attach = [i for i in self.all_attach if i]

    def _check_if_duplicates(self):
        """
        Check if status file contains attachments and plots that are to be sent to avoid emailing duplicates
        """
        self.logger.error('In mails.py: _check_if_duplicates')
        try:
            # Extract bucket name and object key from the S3 URL
            bucket_name, object_key = self.status_file.split('://')[1].split('/', 1)
            
            # Get the filename from the object key
            status_filename = os.path.basename(object_key)
            
            # Create a temporary directory
            with tempfile.TemporaryDirectory() as tmp_dir:
                local_csv_path = os.path.join(tmp_dir, status_filename)
                
                # Download the CSV file from S3 to the local temporary file
                s3_client = boto3.client('s3')
                s3_client.download_file(bucket_name, object_key, local_csv_path)
                
                # Read the local CSV file using pandas
                df = pd.read_csv(local_csv_path)
                
                duplicate_attachments = [
                    i for i in self.attachments if i in df['Attachments'].to_list()]
                duplicate_plots = [
                    i for i in self.plots if i in df['Plots'].to_list()]
                if duplicate_attachments or duplicate_plots:
                    return(True)
                else:
                    return(False)
        except Exception as exc:
            self.logger.error(
                f'Could not read status file {self.status_file} due to {exc}')
            raise exc
    
    def parse_address(email_string):
        """
        Parse an email address from an email string.
        """
        return parseaddr(email_string)[1]
            
    def _send_email_SMTP(self, to, bcc, subject, body_html, body_text, attachments=[], important=False):
        """
        Use SMTP server to send emails through the FishSOOP@unsw.edu.au email address
        """
        self.logger.error('In mails.py: _send_email_SMTP')

        smtp_server = 'SERVER'
        smtp_port = SMTP_PORT
        smtp_username = 'username'
        smtp_password = 'password'

        msg = MIMEMultipart("alternative")
        msg['From'] = self.from_email
        msg['To'] = ', '.join(to)
        msg['Subject'] = subject
        msg['Bcc'] = ', '.join(bcc) if bcc else None
        
        msg.attach(MIMEText(body_text, 'plain'))
        msg.attach(MIMEText(body_html, 'html'))
        
        self.logger.error(f'In mails.py: attachments are: {attachments}')
        
        bucket_name = 'fishsoop-email'
        for attachment in attachments:
            
            folder_name = attachment.split('_')[1]  # Extract the 4 digits following MOANA_
            object_key = f'{folder_name}/{attachment}'  # Construct the object key
            local_file_path = f'/tmp/{attachment}'
            
            # Download the file from S3
            s3 = boto3.client('s3')
            self.logger.error(f'Loading into Lambda file: {bucket_name}/{object_key}')
            s3.download_file(bucket_name, object_key, local_file_path)
            with open(local_file_path, 'rb') as file:
                part = MIMEApplication(file.read(), Name=attachment)
            part['Content-Disposition'] = f'attachment; filename="{attachment}"'
            msg.attach(part)

        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_username, smtp_password)
                #email_body = f"From: {self.from_email}\nTo: {', '.join(to)}\nSubject: {subject}\n\n{body_html}"
                server.sendmail(self.from_email, to, msg.as_string())

                return True
        except Exception as e:
            self.logger.error(f'Error sending email: {e}')
            return False

    def run(self):
        """
        Send email with data, check if email sent, record sent email
        """
        self.logger.error('In mails.py: run')
        try:
            self.logger.error(f'The create_status_file is set to: {self.create_status_file}')
            if self.create_status_file:
                self.logger.error('Lets go create that file')
                self._create_status_file()
            self._get_email_parameters()
            if self.status_file:
                duplicates = self._check_if_duplicates()
            self.logger.error('In mails.py: back in run after _check_if_duplicates')
            if (not duplicates or not self.status_file) and (len(self.all_attach) > 0):
                self.logger.error(
                    f'Emailing {self.recipients} Moana data...')
                self._send_email_SMTP(to=self.recipients, bcc=self.bcc,
                                    subject=self.subject,
                                    body_html=self.html,
                                    body_text=self.text,
                                    attachments=self.all_attach,
                                    important=False)
                
                good_email = self.has_been_alerted()
                self.logger.error('Email sent.')
                
                if good_email and self.status_file:
                    self._record_success()

                if self.status_file:
                    self._record_success()
                    
            else:
                raw_file = self.ds.attrs['raw_data_filename']
                self.logger.error(
                    f'Email not sent because duplicate found in {self.status_file} for {raw_file} or no attachments found.')
        except Exception as exc:
            filename = self.ds.attrs['raw_data_filename']
            self.logger.error(f'Email not sent for {filename} due to {exc}')
