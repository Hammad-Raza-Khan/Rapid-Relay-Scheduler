from apscheduler.schedulers.blocking import BlockingScheduler
import random, csv, smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import boto
import boto.s3.connection
from io import StringIO
import boto3
import pandas as pd



def stream_service():
    random_data = random.randint(1, 100)
    abc = (f"Streaming service is active. Your unique id is: {random_data}")
    print(abc)
    with open('data.csv', 'a') as file:
        writer = csv.writer(file)
        writer.writerow([abc])
    send_email()

def send_email():
    sender_address = ''           //Your email
    sender_pass = ''                //Passkey
    receiver_address = ''              //Receiver's Adrress
    
# Multipurpose Internet Mail Extensions = for ascii and attachments
    message = MIMEMultipart() 
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = 'Streaming Service Data'
    
    # Body, attachment for the mail and setting headers
    message.attach(MIMEText('Please find the attached file.', 'plain'))
    
    # Attaching the file
    attach_file_name = 'data.csv'
    attach_file = open(attach_file_name, 'rb')  # This opens the file as binary mode
    payload = MIMEBase('application', 'octate-stream') # general-purpose MIME type for binary files.
    payload.set_payload((attach_file).read())
    encoders.encode_base64(payload)  # encode the attachment
    payload.add_header('Content-Disposition', 'attachment', filename=attach_file_name)
    message.attach(payload)
    
    # SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587)  
    session.starttls()  # enable security
    session.login(sender_address, sender_pass)  
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()
    print('Mail Sent')

scheduler = BlockingScheduler()
scheduler.add_job(stream_service, 'interval', seconds=30)
scheduler.start()


#Code for AWS S3 bucket
access_key = " "
secret_key = " "

conn = boto.connect_s3(access_key, secret_key)
bucket = conn.create_bucket('')
bucket = conn.get_bucket('')
# bucket = conn.delete_bucket('hammadrazakhann')
#print(bucket)

csv_file = pd.read_csv('data.csv') 
s3 = boto.client('s3', access_key, secret_key)
a = StringIO()
csv_file.to_csv(a, header=True, index=False)
a.seek(0)
s3.put_object(Bucket ='', body =a.getvalue(), Key = 'data.csv')
