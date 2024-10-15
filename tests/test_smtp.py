import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# SMTP server configuration
smtp_server = "smtp.gmail.com"
smtp_port = 587
smtp_user = "nithesh.kumar@blackstraw.ai"
smtp_password = "gxop ifhi buvn gcti"

# Email content
from_email = smtp_user
to_email = "nithesh.kumar@blackstraw.ai"
subject = "Test Email"
body = "This is a test email sent from a Python script."

# Create the email
msg = MIMEMultipart()
msg['From'] = from_email
msg['To'] = to_email
msg['Subject'] = subject
msg.attach(MIMEText(body, 'plain'))

try:
    # Connect to the SMTP server
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.set_debuglevel(1)  # Enable debug output
    server.starttls()  # Upgrade the connection to a secure encrypted SSL/TLS connection
    server.login(smtp_user, smtp_password)
    
    # Send the email
    server.sendmail(from_email, to_email, msg.as_string())
    print("Email sent successfully!")
except Exception as e:
    print(f"Failed to send email: {e}")
finally:
    server.quit()
