import os
from twilio.rest import Client

client = Client(
    os.getenv("TWILIO_ACCOUNT_SID"),
    os.getenv("TWILIO_AUTH_TOKEN")
)

msg = client.messages.create(
    body="Teste direto do Python",
    from_="whatsapp:+14155238886",
    to="whatsapp:+5518996283519"
)

print(msg.sid)