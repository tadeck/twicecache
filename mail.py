from twisted.python import log
import smtplib, traceback
import email.Message

def mail(sender='', to='', subject='', text=''):
    message = email.Message.Message()
    message["To"]      = to
    message["From"]    = sender
    message["Subject"] = subject
    message.set_payload(text)
    mailServer = smtplib.SMTP('mailserver')
    mailServer.sendmail(sender, to, message.as_string())
    mailServer.quit()

def error(msg):
    try:
        log.msg(msg)
        mail.mail(to='admin@mydomain', sender='twice@mydomain', subject='Twice Exception', text=msg)
    except:
        log.msg('Error mailing exception!')
        traceback.print_exc()