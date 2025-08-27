import requests

settings = {
    'TELE_TOKEN': '6433975547:AAGEG-bzmB6Mq4-KfKpq3D6_1Mu774UO79A',
    'TELE_CHAT_ID': '-1002082894699'
}

def send_notification(bot_message):
    num_messages = int(len(bot_message) / 4096) + 1
    bot_messages = bot_message.split("\n")
    batch_size = int(len(bot_messages) / num_messages)
    for i in range(0, len(bot_messages), batch_size):     
    
        lines = bot_messages[i:i+batch_size]
        send_text = 'https://api.telegram.org/bot' + settings['TELE_TOKEN'] + '/sendMessage?chat_id=' + settings['TELE_CHAT_ID'] + '&text=' + "\n".join(lines)
        response = requests.get(send_text)
    return response.json()