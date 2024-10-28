from datetime import datetime, timedelta
import email
import imaplib
import os
from settings.credentials import SENSOR_USERNAME, SENSOR_PASSWORD


class ImapSignInError(Exception):
    pass


class ImapFetchError(Exception):
    pass


class ImapInboxSearchError(Exception):
    pass


def get_satellite_data():
    """
    Function to download Karonga sensor measurements which are send via gmail to malawisensordata@gmail.com
    Files are stored in the data/gauge_data/ folder with karonga_{timestamp} as name
    """
    imapSession = imaplib.IMAP4_SSL("imap.gmail.com")
    typ, accountDetails = imapSession.login(SENSOR_USERNAME, SENSOR_PASSWORD)
    if typ != "OK":
        raise ImapSignInError("Unable to sign in")

    imapSession.select("Inbox")

    today = datetime.now()
    yesterday = today - timedelta(days=2)
    yesterday_date_string = yesterday.strftime("%Y-%m-%d")

    search_query = f'(X-GM-RAW "has:attachment after: {yesterday_date_string}")'
    typ, data = imapSession.search(None, search_query)
    if typ != "OK":
        raise ImapInboxSearchError("Error searching inbox")

    # Iterating over all emails
    filename_list = []
    for msgId in data[0].split():
        typ, messageParts = imapSession.fetch(msgId, "(RFC822)")
        if typ != "OK":
            raise ImapFetchError("Error fetching e-mail.")

        emailBody = messageParts[0][1]
        mail = email.message_from_bytes(emailBody)
        for part in mail.walk():
            if part.get_content_maintype() == "multipart":
                # print part.as_string()
                continue
            if part.get("Content-Disposition") is None:
                # print part.as_string()
                continue
            fileName = part.get_filename()
            if bool(fileName):
                if fileName.endswith(".sbd"):
                    filePath = os.path.join("data/gauge_data/", fileName)
                    filename_list.append(filePath)
                    if not os.path.isfile(filePath):
                        fp = open(filePath, "wb")
                        fp.write(part.get_payload(decode=True))
                        fp.close()
    imapSession.close()
    imapSession.logout()
    return filename_list
