import unittest
from unittest.mock import patch
from kingscripts.analytics import king

class TestSendEmail(unittest.TestCase):
    
    @patch('smtplib.SMTP')
    def test_send_email(self, mock_smtp):
        emailRecipient = 'test@example.com'
        emailRecipientName = 'Test Recipient'
        emailSubject = 'Test Subject'
        emailMessage = 'Test Message'
        nameOfFile = 'test.txt'
        attachment = b'Test attachment'
        
        king.sendEmail(emailRecipient, emailRecipientName, emailSubject, emailMessage, nameOfFile, attachment)
        
        mock_smtp.assert_called_once_with('smtp.gmail.com', 587)
        mock_smtp.return_value.starttls.assert_called_once()
        mock_smtp.return_value.login.assert_called_once_with('operations@kingoperating.com', 'password')
        mock_smtp.return_value.sendmail.assert_called_once_with('operations@kingoperating.com', emailRecipient, 
                                                                 'Subject: {}\n\n{}'.format(emailSubject, emailMessage))
        mock_smtp.return_value.quit.assert_called_once()

if __name__ == '__main__':
    unittest.main()
    print("yay")