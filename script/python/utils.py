#%% libs
import os
import smtplib
import time

import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from sqlalchemy.dialects.mysql import SET


#%%
class SendEmail:
    @staticmethod
    def send_email(in_file_name, in_attach_path, in_subject):
        FROM = 'dite_reporte_ticket_glpi@minedu.gob.pe'
        to = ['GESTIONDEDATOS2@minedu.gob.pe', 'APRENDOENCASA23@minedu.gob.pe']
        #to = ['serviciodeayuda6@minedu.gob.pe']
        subject = in_subject
        body = 'Estimados por favor revisar el archivo adjunto correspondiente a los tickets del GLPI-DITE.'
        firm = '<br/><br/>Saludos cordiales.<h6>Correo generado automáticamente</h6>'
        print(to)
        time.sleep(15)
        body = body + firm

        attachment_name = in_file_name

        # todo Multipart
        message = MIMEMultipart()
        message['From'] = FROM
        message['To'] = ", ".join(to)
        message['Subject'] = subject

        message.attach(MIMEText(body, 'html'))

        attachment_file = open(in_attach_path, 'rb')

        attachment_mime = MIMEBase('application', 'octect-stream')

        attachment_mime.set_payload(attachment_file.read())
        encoders.encode_base64(attachment_mime)
        attachment_mime.add_header('Content-Disposition', "attachment; filename= %s" % attachment_name)

        message.attach(attachment_mime)

        server = "172.19.1.15"
        server = smtplib.SMTP(server)
        # server.starttls()
        # server.ehlo()

        message_text = message.as_string()

        server.sendmail(FROM, to, message_text)
        server.quit()
#%%
