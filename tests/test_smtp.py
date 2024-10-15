import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def test_send_email():
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    smtp_user = "nithesh.kumar@blackstraw.ai"
    smtp_password = "gxop ifhi buvn gcti"
    from_email = smtp_user
    to_email = "nithesh.kumar@blackstraw.ai"
    subject = "SMPT connection for testing is working"
    body = "This is a test email sent from unit tests during the creation of the project"
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.set_debuglevel(1)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(from_email, to_email, msg.as_string())
        server.quit()
    except Exception as e:
        assert False, f"Failed to send email: {e}"