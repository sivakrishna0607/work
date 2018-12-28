from bitbucket.client import Client
client = Client('EMAIL', 'PASSWORD')
response = client.get_user()#get user information

